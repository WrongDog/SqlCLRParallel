using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Transactions;
using System.Data.SqlClient;
using System.Data;

namespace SqlCLRParallel.Model
{
    /// <summary>
    /// item class, which holds sql to be executed in parallel
    /// </summary>
    class ThreadSql
    {
        private string _sql;
        private string _connectionString;
        private Exception _exception;
        private string _key;
        private decimal _runtime;
        private IList<SqlParameter> _parameters;
        private string _output= string.Empty;
        bool _started=false;
        bool _finished = false;
        bool _running;


        private AutoResetEvent _handler;

        /// <summary>
        /// Initializes this class
        /// </summary>
        /// <param name="key"></param>
        /// <param name="connectionString"></param>
        /// <param name="sql"></param>
        public ThreadSql(string key, string connectionString, string sql, IList<SqlParameter> parameters):this(key,connectionString,sql)
        {
            _parameters = parameters;
        }
        public ThreadSql(string key, string connectionString, string sql)
        {
            _key = key;
            _connectionString = connectionString;
            _sql = sql;
        }

        /// <summary>
        /// Returns WaitHandle, this class thread will signal when complete
        /// </summary>
        public WaitHandle Handle
        {
            get { return _handler; }
        }
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
        }
        /// <summary>
        /// Returns total milliseconds sql ran
        /// </summary>
        public decimal RunTime
        {
            get { return _runtime; }
        }
        public string CommandText
        {
            get
            {
                return _sql;
            }
        }
        public string Output
        {
            get
            {
                return _output;
            }
        }
        /// <summary>
        /// Returns true if thread has been started
        /// </summary>
        public bool IsStarted
        {
            get { return _started; }
        }
        public bool Finished
        {
            get { return _finished; }
        }

        private int _commandTimeout = 0;

        /// <summary>
        /// Gets/Sets command timeout for executing the sql
        /// </summary>
        public int CommandTimeout
        {
            get { return _commandTimeout; }
            set { _commandTimeout = value; }
        }

        /// <summary>
        /// Returns true if thread is running
        /// </summary>
        public bool IsRunning
        {
            get { return _running; }
        }

        /// <summary>
        /// Returns key this sql is associated by
        /// </summary>
        public string Key
        {
            get { return _key; }
        }

        /// <summary>
        /// Retruns exception if sql has failed. Will return null if success
        /// </summary>
        public Exception Exception
        {
            get
            {
                return _exception;
            }
        }
        public System.Transactions.IsolationLevel? IsolationLevel { 
            get; 
            set; 
        }
        /// <summary>
        /// Executes sql
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="sql"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="dtx"></param>
        /// <returns></returns>
        private static Exception ExecuteSql(string connectionString, string sql,IList<SqlParameter> parameters,out string output, int commandTimeout, DependentTransaction dtx)
        {
            Exception exception = null;
            output = string.Empty;
            try
            {
                if (dtx != null)
                {
                    //if transaction - execute in transaction scope
                    using (TransactionScope scope = new TransactionScope(dtx))
                    {
                        using (SqlConnection conn = new SqlConnection(connectionString))
                        {
                            //Make sure transaction is placed in DTC
                            IDtcTransaction t = TransactionInterop.GetDtcTransaction(Transaction.Current);
                            conn.Open();
                            int retryCount = 120;
                            while (retryCount > 0)
                            {
                                try
                                {
                                    //Enlist to transaction. Will retry, because sometimes SQL server
                                    //reports that transaction is used by another session.
                                    //This happens not very often, but this is a protection
                                    conn.EnlistTransaction(TransactionInterop.GetTransactionFromDtcTransaction(t));
                                    break;
                                }
                                catch
                                {
                                    retryCount--;
                                    if (retryCount == 0)
                                        throw;
                                    Thread.Sleep(500);
                                }
                            }
                            using (SqlCommand cmd = conn.CreateCommand())
                            {
                                ExecuteSql(cmd, sql, parameters, out output, commandTimeout);
                            }
                        }
                        //commit if success
                        scope.Complete();
                    }
                }
                else
                {
                    using (SqlConnection conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        using (SqlCommand cmd = conn.CreateCommand())
                        {
                            //if not transactional, then just execute sql
                            ExecuteSql(cmd, sql, parameters, out output, commandTimeout);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return exception;
        }
        private static void ExecuteSql(SqlCommand cmd, string sql, IList<SqlParameter> parameters, out string output, int commandTimeout)
        {
            //and let's execute sql
            if (parameters != null) foreach (SqlParameter sqlparam in parameters) cmd.Parameters.Add(sqlparam);
            cmd.CommandTimeout = commandTimeout;
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = sql;
            int row = cmd.ExecuteNonQuery();
            output = string.Format(" Rows affected:{0} ", row);
            if (parameters != null)
            {
                foreach (SqlParameter sqlparam in cmd.Parameters)
                {
                    if (sqlparam.Direction == ParameterDirection.InputOutput) output += string.Format(",{0}:{1}", sqlparam.ParameterName, sqlparam.Value);
                }
            }

        }
        private static Exception ExecuteSqlInNewTransaction(string connectionString, string sql,IList<SqlParameter> parameters,out string output, int commandTimeout,System.Transactions.IsolationLevel isolation)
        {
            Exception exception = null;
            output = string.Empty;
            try
            {
                
                //if transaction - execute in transaction scope
                TransactionOptions option = new TransactionOptions();
                option.IsolationLevel = isolation;
                using (TransactionScope scope = new TransactionScope(TransactionScopeOption.RequiresNew, option))
                {
                    using (SqlConnection conn = new SqlConnection(connectionString))
                    {
                        conn.Open();
                        using (SqlCommand cmd = conn.CreateCommand())
                        {
                            ExecuteSql(cmd, sql, parameters, out output, commandTimeout);
                        }
                    }
                    //commit if success
                    scope.Complete();
                }
            }
            catch (Exception ex)
            {
                exception = ex;
            }

            return exception;
        }

        /// <summary>
        /// Threaded method to execute sql
        /// </summary>
        /// <param name="state"></param>
        private void Run(object state)
        {
            System.Diagnostics.Stopwatch watch = new System.Diagnostics.Stopwatch();
            watch.Start();
            if (state != null && state.GetType() == typeof(DependentTransaction))
            {
                //if transaction was passed to a thread - create depended transaction scope
                using (DependentTransaction tx = (DependentTransaction)state)
                {
                    _exception = ExecuteSql(this._connectionString, this._sql,this._parameters,out this._output, this.CommandTimeout, tx);
                    tx.Complete();
                }
            }
            else if (state != null && state.GetType() == typeof(System.Transactions.IsolationLevel))
            {
                _exception = ExecuteSqlInNewTransaction(this._connectionString, this._sql, this._parameters, out this._output,this.CommandTimeout, (System.Transactions.IsolationLevel)state);
            }
            else
            {
                //otherwise just execute the sql
                _exception = ExecuteSql(this._connectionString, this._sql, this._parameters, out this._output,this.CommandTimeout, null);
            }

            //calculate run time
            watch.Stop();
            _runtime = Convert.ToDecimal(watch.ElapsedMilliseconds);

            _running = false;
            _finished = true;
            //signal thread end
            _handler.Set();
        }

        private DependentTransaction _dependedTransaction;

        /// <summary>
        /// Indicate that sql is transaction if trans parameter is not null
        /// </summary>
        /// <param name="trans"></param>
        public void AddTransaction(Transaction trans)
        {
            if (trans != null)
                DependentTransaction = trans.DependentClone(DependentCloneOption.BlockCommitUntilComplete);
        }
        /// <summary>
        /// Returns depended transaction for multi-threading
        /// </summary>
        private DependentTransaction DependentTransaction
        {
            get { return _dependedTransaction; }
            set { _dependedTransaction = value; }
        }

        /// <summary>
        /// Start the thread
        /// </summary>
        /// <returns></returns>
        public WaitHandle Start()
        {
            if (_handler == null)
            {
                _handler = new AutoResetEvent(false);
                _started = true;
                _running = true;
                WaitCallback cb = new WaitCallback(Run);
                if (DependentTransaction != null)
                {
                    //if transactional, pass depended transaction
                    ThreadPool.QueueUserWorkItem(cb, DependentTransaction);
                }else if(IsolationLevel.HasValue){

                    ThreadPool.QueueUserWorkItem(cb, IsolationLevel.Value);
                }
                else
                {
                    //if not, pass null
                    ThreadPool.QueueUserWorkItem(cb);
                }
            }
            else
            {
                throw new InvalidOperationException(string.Format("Thread '{0}' is already started.", this.Key));
            }

            return _handler;
        }
    }

}
