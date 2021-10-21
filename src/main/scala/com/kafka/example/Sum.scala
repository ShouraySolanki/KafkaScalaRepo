package com.kafka.example


class Sum {
  private var a = 0
  private var b = 0
  private var sum = a + b

  def getB: Int = b

  def getSum: Int = sum

  def setSum(sum: Int): Unit = {
    this.sum = sum
  }

  def getA: Int = a

  def setA(a: Int): Unit = {
    this.a = a
  }

  def getB(b: Int): Int = this.b

  def setB(b: Int): Unit = {
    this.b = b
  }

  override def toString: String = "Sum{" + "a=" + a + ", b=" + b + ", sum=" + sum + '}'
}

