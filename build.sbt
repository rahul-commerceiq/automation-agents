ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.boomerang"

lazy val root = (project in file("."))
  .settings(
    name := "spark-size-calculator",
    libraryDependencies ++= Seq(
      // Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
      
      // Configuration
      "com.typesafe" % "config" % "1.4.2",
      
      // Logging
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.apache.spark" %% "spark-core" % "3.4.1" % Test,
      "org.apache.spark" %% "spark-sql" % "3.4.1" % Test
    ),
    
    // Assembly plugin settings
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "application.conf" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    
    // Exclude Spark from assembly JAR since it's provided
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { jar =>
        jar.data.getName.contains("spark-core") ||
        jar.data.getName.contains("spark-sql") ||
        jar.data.getName.contains("hadoop")
      }
    }
  )