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

import scopt.OptionParser

/**
 * This is a class for configuration provided by the command line parameters
 *
 * Note: scopt requires all fields to have default values.
 *       Even if a field is mandatory it needs a default value.
 */
case class CmdConfig(
  sourceFormat:   String  = "",
  targetFormat:   String  = "",
  sourcePath:     String  = "",
  targetPath:     String  = "",
  numPartitions:  Int     = 0,
  numDriverCores: Int     = 4,
  inferSchema:    Boolean = true,
  csvHeader:      Boolean = true,
  csvQuote:       String  = "\"",
  csvEscape:      String  = "\\",
  charset:        String  = "UTF-8",
  csvComment:     String  = "#")

object CmdConfig {

  val supportedFormats = Seq("parquet", "csv", "json", "orc", "text")
  val supportedProtocols = Seq("file://", "hdfs://", "s3://")

  def getCmdLineArguments(args: Array[String]): CmdConfig = {
    val parser = new CmdParser("java -cp simple-parquet-convertor.jar africa.absa.parquet.convertor.SimpleParquetConvertor")

    val optionCmd = parser.parse(args, CmdConfig())
    if (optionCmd.isEmpty) {
      // Wrong arguments provided, the message is already displayed
      System.exit(1)
    }
    optionCmd.get
  }

  private class CmdParser(programName: String) extends OptionParser[CmdConfig](programName) {

    head("Simple Parquet Convertor", "")

    private def readableDataFormats = CmdConfig.supportedFormats.mkString(", ")
    private def readableProtocols = CmdConfig.supportedProtocols.mkString(", ")
    private def getSupportedFormatsMessage = s"Supported formats are: $readableDataFormats"
    private def getSupportedProtocolsMessage = s"Supported protocols are: $readableProtocols"

    opt[String]("source-format").required().action((value, config) =>
      config.copy(sourceFormat = value)).text(s"[REQUIRED] Source data format. $getSupportedFormatsMessage").validate(format =>
      if (CmdConfig.supportedFormats.contains(format)) success
      else failure(s"$format is not a supported data format. $getSupportedFormatsMessage"))

    opt[String]("target-format").required().action((value, config) =>
      config.copy(targetFormat = value)).text(s"[REQUIRED] Target data format. $getSupportedFormatsMessage").validate(format =>
      if (CmdConfig.supportedFormats.contains(format)) success
      else failure(s"$format is not a supported data format. $getSupportedFormatsMessage"))

    opt[String]("source-path").required().action((value, config) =>
      config.copy(sourcePath = value)).text(s"[REQUIRED] The path of source file/folder to be converted. $getSupportedProtocolsMessage")

    opt[String]("target-path").required().action((value, config) =>
      config.copy(targetPath = value)).text(s"[REQUIRED] The path of target folder to save the converted data into. $getSupportedProtocolsMessage")

    opt[Boolean]("infer-schema").optional().action((value, config) =>
      config.copy(inferSchema = value)).text("[OPTIONAL] This options attempts to infer strongly typed schema from (un/looser)-typed data sources")

    opt[Boolean]("csv-header").optional().action((value, config) =>
      config.copy(csvHeader = value)).text("[OPTIONAL] This options specifies whether CSV files (for read and write) have headers")

    opt[String]("csv-quote").optional().action((value, config) =>
      config.copy(csvQuote = value)).text("[OPTIONAL] This options specifies the CSV quote character. Default is \"")

    opt[String]("csv-escape").optional().action((value, config) =>
      config.copy(csvEscape = value)).text("[OPTIONAL] This options specifies the CSV escape character. Default is \\")

    opt[String]("csv-comment").optional().action((value, config) =>
      config.copy(csvComment = value)).text("[OPTIONAL] This options specifies the CSV comment character. Default is #")

    opt[String]("charset").optional().action((value, config) =>
      config.copy(charset = value)).text("[OPTIONAL] This options specifies the charset for text based data formats. Default is UTF-8")

    opt[Int]("num-partitions").optional().action((value, config) =>
      config.copy(numPartitions = value)).text("[OPTIONAL] ADVANCED: Number of partitions (files) produced. Higher the number, higher the parallelizm achievable. Causes shuffling.")

    opt[Int]("num-driver-cores").optional().action((value, config) =>
      config.copy(numDriverCores = value)).text("[OPTIONAL] Number of CPU cores to be used for the conversion (driver side)")
  }
}
