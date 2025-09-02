package com.boomerang.spark

import com.boomerang.spark.config.JobConfig
import com.boomerang.spark.utils.{SizeCalculator, SizeReport}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.LogManager

object SizeCalculatorJob {
  
  private val logger = LogManager.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    
    // Parse configuration
    val config = JobConfig.fromArgs(args)
    
    logger.info(s"Starting Spark Size Calculator Job with config: $config")
    
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("Data Size Calculator")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    try {
      // Initialize size calculator
      val calculator = new SizeCalculator(spark)
      
      logger.info(s"Calculating sizes for path: ${config.inputPath}")
      
      // Calculate sizes
      val report = calculator.calculateSizes(
        inputPath = config.inputPath,
        includeHidden = config.includeHiddenFiles,
        recursive = config.recursive,
        calculateChecksums = config.calculateChecksums,
        groupByExtension = config.groupByExtension
      )
      
      // Log summary
      logSummary(report)
      
      // Save results
      saveReport(spark, report, config)
      
      logger.info("Job completed successfully")
      
    } catch {
      case ex: Exception =>
        logger.error(s"Job failed with error: ${ex.getMessage}", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }
  
  private def logSummary(report: SizeReport): Unit = {
    logger.info("=== Size Calculation Summary ===")
    logger.info(s"Total Files: ${report.totalFiles}")
    logger.info(s"Total Directories: ${report.totalDirectories}")
    logger.info(s"Total Size: ${formatBytes(report.totalSize)}")
    logger.info(s"Average File Size: ${formatBytes(report.averageFileSize.toLong)}")
    
    report.largestFile.foreach { file =>
      logger.info(s"Largest File: ${file.name} (${formatBytes(file.size)})")
    }
    
    report.smallestFile.foreach { file =>
      logger.info(s"Smallest File: ${file.name} (${formatBytes(file.size)})")
    }
    
    if (report.extensionStats.nonEmpty) {
      logger.info("=== Extension Statistics ===")
      report.extensionStats.toSeq.sortBy(-_._2._2).take(10).foreach { case (ext, (count, size)) =>
        logger.info(s".$ext: $count files, ${formatBytes(size)}")
      }
    }
    
    logger.info(s"Processing Time: ${report.processingTime}ms")
  }
  
  private def saveReport(spark: SparkSession, report: SizeReport, config: JobConfig): Unit = {
    import spark.implicits._
    
    // Create summary DataFrame
    val summaryData = Seq(
      ("total_files", report.totalFiles.toString),
      ("total_directories", report.totalDirectories.toString),
      ("total_size_bytes", report.totalSize.toString),
      ("total_size_formatted", formatBytes(report.totalSize)),
      ("average_file_size_bytes", report.averageFileSize.toLong.toString),
      ("average_file_size_formatted", formatBytes(report.averageFileSize.toLong)),
      ("processing_time_ms", report.processingTime.toString)
    )
    
    val summaryDF = summaryData.toDF("metric", "value")
    
    // Create extension statistics DataFrame if available
    val extensionDF = if (report.extensionStats.nonEmpty) {
      val extensionData = report.extensionStats.map { case (ext, (count, size)) =>
        (ext, count, size, formatBytes(size))
      }.toSeq
      extensionData.toDF("extension", "file_count", "total_size_bytes", "total_size_formatted")
    } else {
      spark.emptyDataFrame
    }
    
    // Save based on output format
    config.outputFormat.toLowerCase match {
      case "json" =>
        summaryDF.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .json(s"${config.outputPath}/summary")
        
        if (extensionDF.count() > 0) {
          extensionDF.coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .json(s"${config.outputPath}/extensions")
        }
        
      case "csv" =>
        summaryDF.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .option("header", "true")
          .csv(s"${config.outputPath}/summary")
        
        if (extensionDF.count() > 0) {
          extensionDF.coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(s"${config.outputPath}/extensions")
        }
        
      case "parquet" =>
        summaryDF.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(s"${config.outputPath}/summary")
        
        if (extensionDF.count() > 0) {
          extensionDF.coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .parquet(s"${config.outputPath}/extensions")
        }
        
      case _ =>
        throw new IllegalArgumentException(s"Unsupported output format: ${config.outputFormat}")
    }
    
    logger.info(s"Results saved to: ${config.outputPath}")
  }
  
  private def formatBytes(bytes: Long): String = {
    val units = Array("B", "KB", "MB", "GB", "TB")
    var size = bytes.toDouble
    var unitIndex = 0
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024
      unitIndex += 1
    }
    
    f"$size%.2f ${units(unitIndex)}"
  }
}