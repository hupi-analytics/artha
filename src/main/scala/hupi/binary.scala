import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner

import com.typesafe.config._
import scala.collection.JavaConverters._

import org.joda.time._


object artha_binary {

  // Determiner les coordonnées des sensors
  def getCoordinatesSensors (sensor: Short) : Array[Short] = {
    if (sensor == 0) {
      return Array(0.toShort, 0.toShort)
    } else if (sensor == 1) {
      return Array(1.toShort, 1.toShort)
    } else if (sensor == 2) {
      return Array(1.toShort, 2.toShort)
    } else if (sensor == 3) {
      return Array(1.toShort, 3.toShort)
    } else if (sensor == 4) {
      return Array(1.toShort, 4.toShort)
    } else if (sensor == 5) {
      return Array(1.toShort, 6.toShort)
    } else if (sensor == 6) {
      return Array(1.toShort, 10.toShort)
    } else if (sensor == 7) {
      return Array(1.toShort, 12.toShort)
    } else if (sensor == 8) {
      return Array(1.toShort, 13.toShort)
    } else if (sensor == 9) {
      return Array(1.toShort, 14.toShort)
    } else if (sensor == 10) {
      return Array(1.toShort, 15.toShort)
    } else if (sensor == 11) {
      return Array(2.toShort, 8.toShort)
    } else if (sensor == 12) {
      return Array(5.toShort, 8.toShort)
    } else if (sensor == 13) {
      return Array(6.toShort, 8.toShort)
    } else if (sensor == 14) {
      return Array(7.toShort, 8.toShort)
    } else if (sensor == 15) {
      return Array(8.toShort, 8.toShort)
    } else if (sensor == 16) {
      return Array(1.toShort, 5.toShort)
    } else if (sensor == 17) {
      return Array(1.toShort, 7.toShort)
    } else if (sensor == 18) {
      return Array(1.toShort, 9.toShort)
    } else if (sensor == 19) {
      return Array(1.toShort, 11.toShort)
    } else if (sensor == 20) {
      return Array(3.toShort, 8.toShort)
    } else {
      return Array(4.toShort, 8.toShort)
    }  
  }

  def main(args: Array[String]) {

    val nb_args = 3
  
    if (args.length != nb_args) {
      Console.err.println("Main need " + nb_args + " arguments.")
      System.exit(1)
    }

    val numPartitions = args(0).toInt
    val numSim = args(1).toInt // nombre des fichiers simulés à prendre dans directorySimulation
    val nbGroupToSave = args(2).toInt // nombre des groupes à sauvegarder chaque fois 
 
    val config = ConfigFactory.load()
    val hdfsPath = config.getString("hdfsPath")
    val inputPath = config.getString("inputPath")
    val outPutPath = config.getString("outPutPath") 
    val directorySimulation = config.getString("directorySimulationSim")

    /*
    val hdfsPath = config.getString("hdfsPath")
    val inputPath = config.getString("inputPath")
    val outPutPath = config.getString("outPutPath") + outputFormat
    val directorySimulation = config.getString("directorySimulationSim")
    */

    // SparkSession 
    val sparkSession = SparkSession.builder()
          .appName("artha_binary") 
          .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._

    // Checkpointing
    //sc.setCheckpointDir("target")

   // On voit si le fichier existe déjà dans HDFS, si oui, on le supprime
    val conf = new Configuration();
    conf.set("fs.defaultFS", hdfsPath)
    val fs = FileSystem.get(conf)

    if(fs.exists(new Path(outPutPath)))
      fs.delete(new Path(outPutPath),true)

    val byteOrder = ByteOrder.LITTLE_ENDIAN

    // function to read binary file and get a array of Short as result
    def getArrayShort (input: org.apache.hadoop.fs.Path) : Array[Short] = {
      val stream = fs.open(input)
      val nb_bytes = stream.available()
      val array_bytes = new Array[Byte](nb_bytes)
      // read the full data into the buffer
      val allBytes = stream.readFully(array_bytes);
      // convert to array of couple bytes 
      val array_couple_bytes = array_bytes.grouped(2).toArray
      // Convertir en short
      val array_data = array_couple_bytes.map(array => ByteBuffer.wrap(array).order(byteOrder).getShort)
      // remove all number before first 22
      val index_first_22 = array_data.indexOf(22)
      // On supprime dans array_data les int avant index_first_22
      val array_s = array_data.drop(index_first_22)
      return array_s
    }

    // Get all paths inside directory in HDFS
    val pathSim = fs.listStatus(new Path(directorySimulation)).map(l => l.getPath)

    val ArrayOfShortSim = new ListBuffer[Array[Short]]()

    for (i <- 0 to numSim-1) {
      val arrayS = getArrayShort(pathSim(i))
      ArrayOfShortSim += arrayS
    }

    val array_short = ArrayOfShortSim.toArray.flatten
    
    // Get event_ts with fileName
    val fileName = inputPath.split("/").last
    val myDay = fileName.substring(0,4) + "-" + fileName.substring(4,6) + "-" + fileName.substring(6,8) 
    val myHour = fileName.substring(9,11) + ":" + fileName.substring(11,13) + ":" + fileName.substring(13,15) 

    val event_ts = DateTime.parse(myDay + "T" + myHour).getMillis() / 1000

    val n = array_short.length

     // initialiser les variables
    var finalResult = new ListBuffer[(Double, Short, Short, Short, Short)]()
    var number_measures = 0
    var cpt_measures = 0
    var sumOfNumberValues = 0
    var i = 0
    var nbOccurrences22 = 0 

    while (i < n) {
      val firstValue = array_short(i)
      if (firstValue == 22) {
        val secondValue = array_short(i+1)
        val thirdValue = array_short(i+2)
        val fourthValue = array_short(i+3)
        if (secondValue == 0 && thirdValue > 0 && fourthValue == 0) {
          val old_number_measures = number_measures
          number_measures = thirdValue
          if (i == 0) {
            sumOfNumberValues = 0
          } else {
            sumOfNumberValues = sumOfNumberValues + old_number_measures
          }
          // increment i 
          i = i + 4
          // increment nbOccurrences22
          nbOccurrences22 = nbOccurrences22 + 1
          if (nbOccurrences22 == nbGroupToSave + 1) {
            // on sauvegarde le finalResult (qui contient en principe nbGroupToSave groupes valeurs) dans HDFS
            sc.parallelize(finalResult.toList, numPartitions).toDF("event_timestamp", "sensor_id", "x", "y", "value_sensor").repartition(numPartitions)
            .write.mode(SaveMode.Append).save(hdfsPath + outPutPath)        
            // on initialise finalResult et nbOccurrences22 
            finalResult = new ListBuffer[(Double, Short, Short, Short, Short)]()
            nbOccurrences22 = 0
          } 
        } else {
          i = i + 1
        }
      } else {   
        for (sensor_id <- 0 to 21) {
          for (cpt_measures <- 0 to number_measures-1) { 
            // on calcule event_timestamp (on incrémente 100 microsecond = 0.1 milisecond pour chaque nouvelle donnée de sensor)
            val instant = (event_ts + 0.1 * (cpt_measures + sumOfNumberValues)).toDouble
            // Creer data pour écrire
            val dataSaved = (instant, sensor_id.toShort, getCoordinatesSensors(sensor_id.toShort)(0), getCoordinatesSensors(sensor_id.toShort)(1), array_short(i))
            // append to finalResult
            finalResult += dataSaved
            // check if i = n -1 ou pas => si oui, on enregistre dernier finalResult dans HDFS
            if (i == n - 1) {
              sc.parallelize(finalResult.toList, numPartitions).toDF("event_timestamp", "sensor_id", "x", "y", "value_sensor").repartition(numPartitions)
              .write.mode(SaveMode.Append).save(hdfsPath + outPutPath) 
              // increment i
              i = i + 1
            } else {
              // increment i
              i = i + 1
            }
          }
          // initialiser cpt_measures pour next_sensor
          cpt_measures = 0
        }
      }
    }
  }
}

