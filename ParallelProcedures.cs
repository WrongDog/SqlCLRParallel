using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections.Generic;
using System.Threading;
using System.Collections;
using System.Transactions;
using SqlCLRParallel;
using SqlCLRParallel.Model;


public partial class StoredProcedures
{
    
    [ThreadStatic]
    private static ParallelBlock _block;
    private static Dictionary<string, StoredProcedureParameterDefinitionList> storedProcedureParameters = new Dictionary<string, StoredProcedureParameterDefinitionList>();

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_Declare([SqlFacet(MaxSize = 50)] SqlString name)
    {
        _block = new ParallelBlock(name);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="mode">
        //     Volatile data can be read but not modified, and no new data can be added
        //     during the transaction.
        //Serializable = 0,
        //
        //     Volatile data can be read but not modified during the transaction. New data
        //     can be added during the transaction.
        //RepeatableRead = 1,
        //
        //     Volatile data cannot be read during the transaction, but can be modified.
        //ReadCommitted = 2,
        //
        //     Volatile data can be read and modified during the transaction.
        //ReadUncommitted = 3,
        //
        //     Volatile data can be read. Before a transaction modifies data, it verifies
        //     if another transaction has changed the data after it was initially read.
        //     If the data has been updated, an error is raised. This allows a transaction
        //     to get to the previously committed value of the data.
        //Snapshot = 4,
        //
        //     The pending changes from more highly isolated transactions cannot be overwritten.
        //Chaos = 5,
        //
        //     A different isolation level than the one specified is being used, but the
        //     level cannot be determined. An exception is thrown if this value is set.
        //Unspecified = 6,
    /// </param>
    /// <param name="mode"></param>
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_UseTransaction(
        [SqlFacet(MaxSize=20, IsNullable=false)] SqlString isolationLevel, 
        SqlBoolean seperatedTransaction)
    {
        EnsureBlockDeclared();
        System.Transactions.IsolationLevel level = (System.Transactions.IsolationLevel)Enum.Parse(
            typeof(System.Transactions.IsolationLevel), isolationLevel.Value);
        _block.StartTransaction(level, seperatedTransaction.IsTrue);
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_SetOption_CommandTimeout(int commandTimeout)
    {
        EnsureBlockDeclared();
        _block.CommandTimeout = commandTimeout;
    }

    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_SetOption_MaxThreads(int maxThreads)
    {
        EnsureBlockDeclared();
        _block.MaxThreads = maxThreads;
    }
    
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_AddSql([SqlFacet(MaxSize=128, IsNullable=false)] SqlString key, SqlChars sql)
    {
        EnsureBlockDeclared();
        _block.Add(key.Value, new string(sql.Value),null);
    }
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_AddSqlBatch(
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString keyprefix, 
        SqlChars sql,
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString parameter,
        SqlChars parameterValues)
    {
        EnsureBlockDeclared();
        if (!parameterValues.IsNull)
        {
            foreach (string value in new string(parameterValues.Value).Split(','))
            {
                _block.Add(keyprefix.Value + " " + parameter.Value + ":" + value, new string(sql.Value).Replace(parameter.Value, value),null);
            }
        }
    }
    /// <summary>
    /// 
    /// </summary>
    /// <returns></returns>
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static int Parallel_Execute()
    {
        EnsureBlockDeclared();
        int count = 0;
        if (_block.IsTransactional && !_block.InSeperatedTransaction)
        {
            //may be we need to modify this part to give an option for join current transaction
            TransactionOptions opt = new TransactionOptions();
            opt.IsolationLevel = _block.TransactionLevel;
            using (TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew, opt))
            {
                TransactionInterop.GetTransmitterPropagationToken(Transaction.Current);
                count = _block.Run(Transaction.Current);
                if (count == 0)
                    scope.Complete();
            }
        }
        else
        {
            count = _block.Run(null);
        }
        return count;
    }

    [Microsoft.SqlServer.Server.SqlFunction(FillRowMethodName = "Parallel_Result_FillRow",
        TableDefinition = "key_s nvarchar(100), success_f bit, run_time_ms int, error_s nvarchar(max), error_stack nvarchar(max), output nvarchar(max)")]
    public static IEnumerable Parallel_GetExecutionResult()
    {
        EnsureBlockDeclared();
        ResultItem[] result = _block.GetResult();
        return result;
    }

    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlChars Parallel_GetErrorMessage()
    {
        EnsureBlockDeclared();
        string errorMessage = string.Empty;
        ResultItem[] result = _block.GetResult();
        foreach (ResultItem item in result)
        {
            if (item.ErrorMessage.Length > 0)
                errorMessage += string.Format("{0}: {1}\n\r", item.Key, item.ErrorMessage);
        }

        if (errorMessage.Length == 0)
            return SqlChars.Null;

        return new SqlChars(string.Format("Parallel block '{0}' failed with following errors:\n\r{1}", _block.Name, errorMessage));
    }
    /// <summary>
    /// FillRowMethodName = "Parallel_Result_FillRow"
    /// </summary>
    /// <param name="obj"></param>
    /// <param name="key_s"></param>
    /// <param name="success_f"></param>
    /// <param name="run_time_ms"></param>
    /// <param name="error_s"></param>
    /// <param name="error_stack"></param>
    private static void Parallel_Result_FillRow(object obj, out string key_s, out bool success_f, out int run_time_ms, out string error_s, out string error_stack, out string output)
    {
        ResultItem item = obj as ResultItem;
        key_s = item.Key;
        success_f = item.Success;
        run_time_ms = item.RunTime;
        error_s = item.ErrorMessage;
        error_stack = item.ErrorStack;
        output = item.Output;
    }

    private static void EnsureBlockDeclared()
    {
        if (_block == null)
            throw new ArgumentNullException("Parallel block has not been declared. Please execute Parallel_Declare stored procedure first.");
    }

    /// <summary>
    /// add parameter for stored procedure
    /// </summary>
    /// <param name="storedProcedureName"></param>
    /// <param name="parameterName"></param>
    /// <param name="sqlDatatype"></param>
    /// <param name="IsOutput"></param>
    /// <param name="parameterValues"></param>
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_AddStoredProcedureParameter(
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString storedProcedureKey,
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString parameterName,
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString sqlDatatype,
        SqlChars parameterValues,
        [SqlFacet(IsNullable = true)] SqlBoolean isOutput
        )
    {
        EnsureBlockDeclared();
        StoredProcedureParameterDefinitionList paramList = new StoredProcedureParameterDefinitionList();
        if (storedProcedureParameters.ContainsKey(storedProcedureKey.Value))
        {
            paramList = storedProcedureParameters[storedProcedureKey.Value];

        }
        else
        {
            storedProcedureParameters.Add(storedProcedureKey.Value, paramList);
        }
        paramList.Add(
            new StoredProcedureParameterDefinition(
                 parameterName.Value,
                 sqlDatatype.Value,
                 isOutput.IsNull ? false : isOutput.Value,
                new string(parameterValues.Value)
            )
        );


    }
    
    /// <summary>
    /// must called after Parallel_AddStoredProcedureParameters
    /// </summary>
    /// <param name="storedProcedureName"></param>
    /// <param name="commandText"></param>
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void Parallel_AddStoredProcedure(
        [SqlFacet(MaxSize = 128, IsNullable = false)] SqlString storedProcedureKey,
        SqlChars commandText
        )
    {
        EnsureBlockDeclared();
        StoredProcedureParameterDefinitionList paramList = storedProcedureParameters[storedProcedureKey.Value];
        storedProcedureParameters.Remove(storedProcedureKey.Value);
        if (paramList == null || paramList.Count == 0) throw new Exception(string.Format("stored procedure{0} does not have ant parameters",storedProcedureKey.Value));
        List<SqlParameter> sqlparams=null;
        do
        {
            sqlparams = paramList.GetNext();
            if (sqlparams != null)
            {
                string paramsStr = string.Empty;
                foreach (SqlParameter sqlparam in sqlparams)
                {
                    paramsStr += string.Format(",{0}:{1}", sqlparam.ParameterName, sqlparam.Value);
                }
                _block.Add(storedProcedureKey.Value + paramsStr, new string(commandText.Value), sqlparams);
            }
        } while (sqlparams != null);
        
    }

};
