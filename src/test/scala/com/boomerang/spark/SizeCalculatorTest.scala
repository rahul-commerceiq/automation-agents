package com.boomerang.spark

import com.boomerang.spark.utils.SizeCalculator
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}

class SizeCalculatorTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  var calculator: SizeCalculator = _
  var tempDir: String = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SizeCalculatorTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()
    
    calculator = new SizeCalculator(spark)
    
    // Create temporary test directory
    tempDir = Files.createTempDirectory("size-calculator-test").toString
    createTestFiles()
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    
    // Clean up test files
    if (tempDir != null) {
      deleteDirectory(new File(tempDir))
    }
  }
  
  private def createTestFiles(): Unit = {
    // Create test directory structure
    val subDir = new File(s"$tempDir/subdir")
    subDir.mkdirs()
    
    // Create test files with different sizes
    createTestFile(s"$tempDir/small.txt", "Hello World")
    createTestFile(s"$tempDir/medium.txt", "A" * 1000)
    createTestFile(s"$tempDir/large.txt", "B" * 10000)
    createTestFile(s"$tempDir/subdir/nested.txt", "Nested content")
    createTestFile(s"$tempDir/.hidden.txt", "Hidden file")
    createTestFile(s"$tempDir/data.json", """{"key": "value"}""")
    createTestFile(s"$tempDir/script.py", "print('Hello Python')")
  }
  
  private def createTestFile(path: String, content: String): Unit = {
    val writer = new PrintWriter(new File(path))
    try {
      writer.write(content)
    } finally {
      writer.close()
    }
  }
  
  private def deleteDirectory(directory: File): Unit = {
    if (directory.exists()) {
      directory.listFiles().foreach { file =>
        if (file.isDirectory) {
          deleteDirectory(file)
        } else {
          file.delete()
        }
      }
      directory.delete()
    }
  }
  
  "SizeCalculator" should "calculate basic size statistics" in {
    val report = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    report.totalFiles should be > 0L
    report.totalSize should be > 0L
    report.averageFileSize should be > 0.0
    report.processingTime should be > 0L
  }
  
  it should "include hidden files when configured" in {
    val reportWithHidden = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = true,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    val reportWithoutHidden = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    reportWithHidden.totalFiles should be > reportWithoutHidden.totalFiles
  }
  
  it should "group files by extension" in {
    val report = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    report.extensionStats should not be empty
    report.extensionStats should contain key "txt"
    report.extensionStats should contain key "json"
    report.extensionStats should contain key "py"
  }
  
  it should "handle non-recursive mode" in {
    val recursiveReport = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    val nonRecursiveReport = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = false,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    recursiveReport.totalFiles should be > nonRecursiveReport.totalFiles
  }
  
  it should "identify largest and smallest files" in {
    val report = calculator.calculateSizes(
      inputPath = tempDir,
      includeHidden = false,
      recursive = true,
      calculateChecksums = false,
      groupByExtension = true
    )
    
    report.largestFile should be defined
    report.smallestFile should be defined
    report.largestFile.get.size should be >= report.smallestFile.get.size
  }
}