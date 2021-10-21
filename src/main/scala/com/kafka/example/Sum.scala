package com.kafka.example


/*case class Sum(@JsonProperty("a") a: Int,
               @JsonProperty("b") b: Int,
               @JsonProperty("sum") sum: Int
              )*/
class Sum{
  private var a: Int = 0
  private var b: Int = 0
  private var sum: Int = a + b

  def getA(): Int ={
     a
  }

  def setA(a : Int){
    this.a = a
  }


  def getB(): Int ={
    b
  }

  def setB(b : Int){
    this.b = b
  }
  def getSum(): Int ={
    sum
  }

  def setSum(sum : Int){
    this.sum = sum
  }


  override def toString: String = "Sum{" + "a=" + a + ", b=" + b + ", sum=" + sum + '}'
}

