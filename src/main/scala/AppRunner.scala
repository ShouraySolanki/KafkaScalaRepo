
import com.kafka.example.Processor


object AppRunner {
  def main(args: Array[String]): Unit = {
    val dataTransformer = new Processor
    System.out.println(dataTransformer.process())
  }
}