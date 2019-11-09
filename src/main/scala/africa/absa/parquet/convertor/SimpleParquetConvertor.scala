/*
 * Copyright 2018-2019 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package africa.absa.parquet.convertor

import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SimpleParquetConvertor {

  def main(args: Array[String]) {
    val cmd: CmdConfig = CmdConfig.getCmdLineArguments(args)
    val logger = LoggerFactory.getLogger(this.getClass)

    logger.info(s"Initializing Apache Spark with ${cmd.numDriverCores} CPU cores")
    val spark = SparkSession.builder().appName(s"Simple Parquet Convertor - ${cmd.sourcePath} -> ${cmd.targetPath}")
      .master(s"local[${cmd.numDriverCores}]")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable", "false")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .getOrCreate()

    logger.info("Applying read options")
    val dfr = spark.read.format(cmd.sourceFormat)
    val withReadOpts = cmd.sourceFormat match {
      case "csv"  => applyCsvReadOptions(dfr, cmd)
      case "json" => applyJsonReadOptions(dfr, cmd)
      case _      => dfr
    }

    logger.info(s"Reading the source data (${cmd.sourceFormat}) - ${cmd.sourcePath}")
    val df = withReadOpts.load(cmd.sourcePath)

    val repartitioned = if (cmd.numPartitions > 0) {
      logger.info(s"Applying repartitioning to ${cmd.numPartitions} partitions")
      df.repartition(cmd.numPartitions)
    } else df

    logger.info("Applying write options")
    val dfw = repartitioned.write.format(cmd.targetFormat)
    val withWriteOpts = cmd.targetFormat match {
      case "csv" => applyCsvWriteOptions(dfw, cmd)
      case _     => dfw
    }

    logger.info("Executing the conversion!")
    withWriteOpts.save(cmd.targetPath)
  }

  private def applyCsvReadOptions(dfr: DataFrameReader, config: CmdConfig) = {
    dfr.option("inferSchema", config.inferSchema)
      .option("charset", config.charset)
      .option("header", config.csvHeader)
      .option("quote", config.csvQuote)
      .option("escape", config.csvEscape)
      .option("comment", config.csvComment)
  }

  private def applyCsvWriteOptions(dfw: DataFrameWriter[_], config: CmdConfig) = {
    dfw.option("header", config.csvHeader)
      .option("quote", config.csvQuote)
      .option("escape", config.csvEscape)
      .option("comment", config.csvComment)
  }

  private def applyJsonReadOptions(dfr: DataFrameReader, config: CmdConfig) = {
    dfr.option("charset", config.charset)
  }
}