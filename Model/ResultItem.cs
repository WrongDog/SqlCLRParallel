using System;
using System.Collections.Generic;
using System.Text;

namespace SqlCLRParallel.Model
{
    /// <summary>
    /// Holds thread execution result
    /// </summary>
    [Serializable]
    class ResultItem
    {
        public ResultItem(ThreadSql sql)
        {
            Key = sql.Key;
            if (sql.Finished)
            {
                Success = sql.Exception == null;
                if (sql.Exception != null)
                {
                    ErrorMessage = sql.CommandText+" " +System.Environment.NewLine
                        + sql.ConnectionString + " " + System.Environment.NewLine
                        + sql.Exception.Message;
                    ErrorStack = string.Format("{0}:\n{1}", sql.Exception.GetType().FullName, sql.Exception.StackTrace);
                }
                else
                {
                    ErrorMessage = string.Empty;
                    ErrorStack = string.Empty;
                }
            }
            else
            {
                Success = false;
                ErrorMessage = "Not Finished";
                ErrorStack = string.Empty;

            }
            RunTime = Convert.ToInt32(sql.RunTime);
            Output = sql.Output;
            
        }
        public string Key;
        public bool Success;
        public int RunTime;
        public string ErrorMessage;
        public string ErrorStack;
        public string Output;
    }

}
