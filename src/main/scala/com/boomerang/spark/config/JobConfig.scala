package com.boomerang.spark.config

import com.typesafe.config.{Config, ConfigFactory}

case class JobConfig(
  inputPath: String,
  outputPath: String,
  outputFormat: String,
  includeHiddenFiles: Boolean,
  recursive: Boolean,
  calculateChecksums: Boolean,
  groupByExtension: Boolean,
  parallelism: Int,
  maxRecordsPerFile: Long,
  cacheIntermediateResults: Boolean
)

object JobConfig {
  
  def fromConfig(config: Config = ConfigFactory.load()): JobConfig = {
    val sparkConfig = config.getConfig("spark")
    
    JobConfig(
      inputPath = sparkConfig.getString("default.input-path"),
      outputPath = sparkConfig.getString("default.output-path"),
      outputFormat = sparkConfig.getString("default.output-format"),
      includeHiddenFiles = sparkConfig.getBoolean("calculation.include-hidden-files"),
      recursive = sparkConfig.getBoolean("calculation.recursive"),
      calculateChecksums = sparkConfig.getBoolean("calculation.calculate-checksums"),
      groupByExtension = sparkConfig.getBoolean("calculation.group-by-extension"),
      parallelism = sparkConfig.getInt("performance.parallelism"),
      maxRecordsPerFile = sparkConfig.getLong("performance.max-records-per-file"),
      cacheIntermediateResults = sparkConfig.getBoolean("performance.cache-intermediate-results")
    )
  }
  
  def fromArgs(args: Array[String], baseConfig: JobConfig = fromConfig()): JobConfig = {
    args.grouped(2).foldLeft(baseConfig) { case (config, Array(key, value)) =>
      key match {
        case "--input-path" => config.copy(inputPath = value)
        case "--output-path" => config.copy(outputPath = value)
        case "--output-format" => config.copy(outputFormat = value)
        case "--include-hidden" => config.copy(includeHiddenFiles = value.toBoolean)
        case "--recursive" => config.copy(recursive = value.toBoolean)
        case "--calculate-checksums" => config.copy(calculateChecksums = value.toBoolean)
        case "--group-by-extension" => config.copy(groupByExtension = value.toBoolean)
        case "--parallelism" => config.copy(parallelism = value.toInt)
        case _ => config
      }
    }
  }
}