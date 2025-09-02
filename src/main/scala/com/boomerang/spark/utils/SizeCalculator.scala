package com.boomerang.spark.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI

case class FileInfo(
  path: String,
  name: String,
  extension: Option[String],
  size: Long,
  modificationTime: Long,
  isDirectory: Boolean,
  checksum: Option[String] = None
)

case class SizeReport(
  totalFiles: Long,
  totalDirectories: Long,
  totalSize: Long,
  averageFileSize: Double,
  largestFile: Option[FileInfo],
  smallestFile: Option[FileInfo],
  extensionStats: Map[String, (Long, Long)], // extension -> (count, total_size)
  processingTime: Long
)

class SizeCalculator(spark: SparkSession) {
  
  import spark.implicits._
  
  def calculateSizes(inputPath: String, 
                    includeHidden: Boolean = false,
                    recursive: Boolean = true,
                    calculateChecksums: Boolean = false,
                    groupByExtension: Boolean = true): SizeReport = {
    
    val startTime = System.currentTimeMillis()
    
    // Get file system
    val fs = FileSystem.get(new URI(inputPath), spark.sparkContext.hadoopConfiguration)
    
    // Collect file information
    val fileInfos = collectFileInfos(fs, new Path(inputPath), includeHidden, recursive, calculateChecksums)
    
    // Convert to DataFrame for analysis
    val fileDF = spark.createDataFrame(fileInfos)
    
    if (fileDF.isEmpty) {
      return SizeReport(0, 0, 0, 0.0, None, None, Map.empty, System.currentTimeMillis() - startTime)
    }
    
    // Cache for multiple operations
    fileDF.cache()
    
    // Calculate basic statistics
    val totalFiles = fileDF.filter(!$"isDirectory").count()
    val totalDirectories = fileDF.filter($"isDirectory").count()
    val totalSize = fileDF.filter(!$"isDirectory").agg(sum($"size")).collect()(0).getLong(0)
    val averageFileSize = if (totalFiles > 0) totalSize.toDouble / totalFiles else 0.0
    
    // Find largest and smallest files
    val filesOnly = fileDF.filter(!$"isDirectory" && $"size" > 0)
    val largestFile = if (filesOnly.count() > 0) {
      Some(filesOnly.orderBy($"size".desc).limit(1).as[FileInfo].collect().head)
    } else None
    
    val smallestFile = if (filesOnly.count() > 0) {
      Some(filesOnly.orderBy($"size".asc).limit(1).as[FileInfo].collect().head)
    } else None
    
    // Extension statistics
    val extensionStats = if (groupByExtension) {
      fileDF
        .filter(!$"isDirectory")
        .groupBy($"extension")
        .agg(
          count("*").as("file_count"),
          sum($"size").as("total_size")
        )
        .collect()
        .map(row => {
          val ext = Option(row.getString(0)).getOrElse("no_extension")
          val count = row.getLong(1)
          val size = row.getLong(2)
          ext -> (count, size)
        })
        .toMap
    } else Map.empty[String, (Long, Long)]
    
    val processingTime = System.currentTimeMillis() - startTime
    
    SizeReport(
      totalFiles = totalFiles,
      totalDirectories = totalDirectories,
      totalSize = totalSize,
      averageFileSize = averageFileSize,
      largestFile = largestFile,
      smallestFile = smallestFile,
      extensionStats = extensionStats,
      processingTime = processingTime
    )
  }
  
  private def collectFileInfos(fs: FileSystem, 
                              path: Path, 
                              includeHidden: Boolean, 
                              recursive: Boolean,
                              calculateChecksums: Boolean): Seq[FileInfo] = {
    
    def getExtension(fileName: String): Option[String] = {
      val lastDot = fileName.lastIndexOf('.')
      if (lastDot > 0 && lastDot < fileName.length - 1) {
        Some(fileName.substring(lastDot + 1).toLowerCase)
      } else None
    }
    
    def calculateChecksum(filePath: Path): Option[String] = {
      if (calculateChecksums && fs.isFile(filePath)) {
        try {
          val inputStream = fs.open(filePath)
          val digest = java.security.MessageDigest.getInstance("MD5")
          val buffer = new Array[Byte](8192)
          var bytesRead = inputStream.read(buffer)
          while (bytesRead != -1) {
            digest.update(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
          inputStream.close()
          Some(digest.digest().map("%02x".format(_)).mkString)
        } catch {
          case _: Exception => None
        }
      } else None
    }
    
    def processPath(currentPath: Path): Seq[FileInfo] = {
      try {
        val fileStatus = fs.getFileStatus(currentPath)
        val fileName = fileStatus.getPath.getName
        
        // Skip hidden files if not included
        if (!includeHidden && fileName.startsWith(".")) {
          return Seq.empty
        }
        
        val fileInfo = FileInfo(
          path = fileStatus.getPath.toString,
          name = fileName,
          extension = if (fileStatus.isDirectory) None else getExtension(fileName),
          size = fileStatus.getLen,
          modificationTime = fileStatus.getModificationTime,
          isDirectory = fileStatus.isDirectory,
          checksum = calculateChecksum(fileStatus.getPath)
        )
        
        val currentFileInfo = Seq(fileInfo)
        
        // Recursively process directories if enabled
        if (fileStatus.isDirectory && recursive) {
          val childPaths = fs.listStatus(currentPath).map(_.getPath)
          val childInfos = childPaths.flatMap(processPath).toSeq
          currentFileInfo ++ childInfos
        } else {
          currentFileInfo
        }
      } catch {
        case ex: Exception =>
          println(s"Error processing path $currentPath: ${ex.getMessage}")
          Seq.empty
      }
    }
    
    processPath(path)
  }
}