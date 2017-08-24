package selva

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.kinesis.model.PutRecordRequest
import java.nio.ByteBuffer

import org.json4s.native.Json
import org.json4s.DefaultFormats
import selva.utils.Logger


object Producer2 {
    @volatile var keepRunning = true
    private val logger = Logger.getInstance(getClass.getSimpleName)

    def main(args: Array[String]): Unit = {

        val kinesisStreamName = "quickstats-development-selva"
        val kinesisEndpointUrl = "https://kinesis.us-west-2.amazonaws.com" // e.g. https://kinesis.us-west-2.amazonaws.com"
        val kinesisRegion = "us-west-2"


        val awsCredentialsProvider = new DefaultAWSCredentialsProviderChain()
        val awsCredentials = awsCredentialsProvider.getCredentials()

        // Create the low-level Kinesis Client from the AWS Java SDK.
        val kinesisClient = AmazonKinesisClientBuilder
          .standard()
          .withEndpointConfiguration(new EndpointConfiguration(kinesisEndpointUrl, kinesisRegion))
          .withCredentials(awsCredentialsProvider)
          .build()


        val mainThread = Thread.currentThread
        Runtime.getRuntime.addShutdownHook(new Thread() {override def run = {
            keepRunning = false
            mainThread.join()
        }})

        // Generate and send the data
        val recordsPerSecond = 20
        var recordNum = 0
        while (keepRunning) {
            for (x <- 1 to recordsPerSecond) {
                recordNum += 1
                val data = generateData(recordNum)
                val partitionKey = s"partitionKey-$recordNum"
                val putRecordRequest = new PutRecordRequest().withStreamName(kinesisStreamName)
                  .withPartitionKey(partitionKey)
                  .withData(ByteBuffer.wrap(data.getBytes()))
                kinesisClient.putRecord(putRecordRequest)
            }
            println("\nSent: " + recordNum + " records")
            Thread.sleep(1000) // Sleep for a second
        }
        logger.info("\nTotal number of records sent: " + recordNum)
    }

    //     Function to generate data
    def generateData(recordNumber: Int): String = {
        val sampleData = Map[String, Any](
            "id" -> recordNumber,
            "date" -> System.currentTimeMillis.toString
        )

        Json(DefaultFormats).write(sampleData)
    }
}
