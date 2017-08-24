package selva

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration
import com.amazonaws.services.kinesis.producer.KinesisProducer

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.Random
import org.json4s.DefaultFormats
import org.json4s.native.Json
import selva.utils.Logger


object Producer {
    private val logger = Logger.getInstance(getClass.getSimpleName)

    private val EXECUTOR = Executors.newScheduledThreadPool(1)

    private val TIMESTAMP = System.currentTimeMillis.toString

    private val SECONDS_TO_RUN = 5

    private val RECORDS_PER_SECOND = 2000

    val STREAM_NAME = "quickstats-development-selva"

    val REGION = "us-west-2"

    def getKinesisProducer: KinesisProducer = {
        val config = new KinesisProducerConfiguration()

        config.setRegion(REGION)

        config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain)

        config.setMaxConnections(1)

        config.setRequestTimeout(60000)

        config.setRecordMaxBufferedTime(15000)

        val producer = new KinesisProducer(config)

        producer
    }

    @throws[Exception]
    def main(args: Array[String]): Unit = {
        val producer = getKinesisProducer
        // The monotonically increasing sequence number we will put in the data of each record
        val sequenceNumber = new AtomicLong(0)
        // The number of records that have finished (either successfully put, or failed)
        val completed = new AtomicLong(0)

        // The lines within run() are the essence of the KPL API.
        val putOneRecord = new Runnable() {
            override def run(): Unit = {
                val data = generateData(sequenceNumber.get)
                // TIMESTAMP is our partition key
                val f = Future {
                    producer.addUserRecord(STREAM_NAME, TIMESTAMP, randomExplicitHashKey, ByteBuffer.wrap(data.getBytes()))
                }
                f.onComplete {
                    case Success(value) => completed.getAndIncrement
                    case Failure(e) => e.printStackTrace
                }
            }
        }


        // This gives us progress updates
        EXECUTOR.scheduleAtFixedRate(new Runnable() {
            override def run(): Unit = {
                val put = sequenceNumber.get
                val total = RECORDS_PER_SECOND * SECONDS_TO_RUN
                val putPercent = 100.0 * put / total
                val done = completed.get
                val donePercent = 100.0 * done / total
                logger.info(s"Put $put of $total so far $putPercent %, $done have completed $donePercent %")
            }
        }, 1, 1, TimeUnit.SECONDS)

        // Kick off the puts
        logger.info(s"Starting puts... will run for $SECONDS_TO_RUN seconds at $RECORDS_PER_SECOND records per second")
        executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, SECONDS_TO_RUN, RECORDS_PER_SECOND)
        EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS)
        logger.info("Waiting for remaining puts to finish...")
        producer.flushSync
        logger.info("All records complete.")
        producer.destroy
        logger.info("Finished.")
    }


    private def executeAtTargetRate(exec: ScheduledExecutorService, task: Runnable, counter: AtomicLong, durationSeconds: Int, ratePerSecond: Int) = {
        exec.scheduleWithFixedDelay(new Runnable() {
            val startTime = System.nanoTime

            override

            def run(): Unit = {
                val secondsRun = (System.nanoTime - startTime) / 1e9
                val targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond
                while ( {
                    counter.get < targetCount
                }) {
                    counter.getAndIncrement
                    try
                        task.run()
                    catch {
                        case e: Exception =>
                            logger.error("Error running task", e)
                            System.exit(1)
                    }
                }
                if (secondsRun >= durationSeconds) exec.shutdown()
            }
        }, 0, 1, TimeUnit.MILLISECONDS)
    }

    private val RANDOM = new Random()

    def randomExplicitHashKey: String = new BigInteger(128, RANDOM).toString(10)


    def generateData(sequenceNumber: Long): String = {
        val sampleData = Map[String, Any](
            "id" -> sequenceNumber,
            "date" -> System.currentTimeMillis.toString
        )
        Json(DefaultFormats).write(sampleData)
    }
}

