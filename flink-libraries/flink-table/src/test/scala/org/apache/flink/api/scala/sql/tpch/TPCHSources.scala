/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.scala.sql.tpch

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, SqlTimeTypeInfo}
import org.apache.flink.table.api.BatchTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

object TPCHSources {

  def registerAll(tpchBaseDir: String, tableEnv: BatchTableEnvironment): Unit = {

    registerCustomer(s"$tpchBaseDir/customer.tbl", tableEnv)
    registerLineitem(s"$tpchBaseDir/lineitem.tbl", tableEnv)
    registerNation(s"$tpchBaseDir/nation.tbl", tableEnv)
    registerOrders(s"$tpchBaseDir/orders.tbl", tableEnv)
    registerPart(s"$tpchBaseDir/part.tbl", tableEnv)
    registerPartSupp(s"$tpchBaseDir/partsupp.tbl", tableEnv)
    registerRegion(s"$tpchBaseDir/region.tbl", tableEnv)
    registerSupplier(s"$tpchBaseDir/supplier.tbl", tableEnv)

  }

  def registerCustomer(customerFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val customer = new CsvTableSource(
      customerFile,
      Array(
        "C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY", "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT",
        "C_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("customer", customer)
  }

  def registerLineitem(lineitemFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val lineitem = new CsvTableSource(
      lineitemFile,
      Array(
        "L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY", "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE",
        "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE",
        "L_RECEIPTDATE", "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        SqlTimeTypeInfo.DATE,
        SqlTimeTypeInfo.DATE,
        SqlTimeTypeInfo.DATE,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("lineitem", lineitem)
  }

  def registerNation(nationFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val nation = new CsvTableSource(
      nationFile,
      Array("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("nation", nation)
  }

  def registerOrders(ordersFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val orders = new CsvTableSource(
      ordersFile,
      Array(
        "O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE",
        "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        SqlTimeTypeInfo.DATE,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("orders", orders)
  }

  def registerPart(partFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val part = new CsvTableSource(
      partFile,
      Array(
        "P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE", "P_SIZE", "P_CONTAINER",
        "P_RETAILPRICE", "P_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("part", part)
  }

  def registerPartSupp(partSuppFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val partsupp = new CsvTableSource(
      partSuppFile,
      Array("PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY", "PS_SUPPLYCOST", "PS_COMMENT")
        .map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("partsupp", partsupp)
  }

  def registerRegion(regionFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val region = new CsvTableSource(
      regionFile,
      Array("R_REGIONKEY", "R_NAME", "R_COMMENT").map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("region", region)
  }

  def registerSupplier(supplierFile: String, tableEnv: BatchTableEnvironment): Unit = {

    val supplier = new CsvTableSource(
      supplierFile,
      Array("S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY", "S_PHONE", "S_ACCTBAL", "S_COMMENT")
        .map(_.toLowerCase()),
      Array(
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.LONG_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "|"
    )

    tableEnv.registerTableSource("supplier", supplier)
  }



}
