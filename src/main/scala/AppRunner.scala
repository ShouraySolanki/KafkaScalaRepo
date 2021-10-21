
import com.kafka.example.DataTransformer


object AppRunner {
  def main(args: Array[String]): Unit = {
    val dataTransformer = new DataTransformer
    System.out.println(dataTransformer.getSendTransform)
  }
}