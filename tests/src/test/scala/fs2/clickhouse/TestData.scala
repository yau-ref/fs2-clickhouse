package fs2.clickhouse

object TestData {

  // testing data
  case class User(name: String, age: Int)

  def users(num: Int = 1000): Vector[User] =
    (0 to num)
      .map(i => User(s"user-$i", i % 100))
      .toVector


}
