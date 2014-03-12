//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;

namespace SqlCLRParallel
{
    internal class StoredProcedureParameterDefinitionList : List<StoredProcedureParameterDefinition>
    {
        public new void Add(StoredProcedureParameterDefinition item)
        {
            foreach (StoredProcedureParameterDefinition definition in this)
            {
                if (definition.ParameterName == item.ParameterName) throw new Exception("can not have parameter with same name as "+item.ParameterName);
            }
            base.Add(item);

        }
        private List<int> index = null;
        public List<SqlParameter> GetNext()
        {
            if (index == null)
            {
                index = new List<int>();
                for (int idx = 0; idx < this.Count; idx++) index.Add(0);
            }
            else
            {
                int increment = 0;
                bool finished = false;
                while (!finished)
                {
                    index[increment]++;
                    if (index[increment] < this[increment].Values.Count)
                    {
                        finished = true;
                    }
                    else
                    {
                        index[increment] = 0;
                        increment++;
                        if (increment >= index.Count) return null;
                    }
                }
            }
            return GetByIndex();
        }
        private List<SqlParameter> GetByIndex()
        {
            List<SqlParameter> sqlparams = new List<SqlParameter>();
            for (int idx =0;idx<Count;idx++)
            {
                StoredProcedureParameterDefinition def = this[idx];
                SqlParameter sqlparam = new SqlParameter(def.ParameterName, def.SqlDbType);
                sqlparam.SqlValue =  def.Values[index[idx]];//convert?
                sqlparam.Direction = def.IsOutput ? ParameterDirection.InputOutput : ParameterDirection.Input;
                sqlparams.Add(sqlparam);

            }
            return sqlparams;
        }
    }
}
