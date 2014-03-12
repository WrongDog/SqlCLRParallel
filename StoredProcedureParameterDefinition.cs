//------------------------------------------------------------------------------
// <copyright file="CSSqlClassFile.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace SqlCLRParallel
{
    internal class StoredProcedureParameterDefinition
    {
        public StoredProcedureParameterDefinition(
            string parameterName,
            string sqlDbType,
            bool isOutput,
            string values
            ) { 
            this.ParameterName=parameterName;
            this.SqlDbType = (SqlDbType)Enum.Parse(typeof(SqlDbType), sqlDbType, true);
            this.IsOutput = isOutput;
            this.Values = new List<string>(values.Split(','));
        }
        public string ParameterName { get; set; }
        public SqlDbType SqlDbType { get; set; }
        public bool IsOutput { get; set; }
        public List<string> Values { get; set; }

    }
}
