package fs2.clickhouse

import org.scalacheck.Gen

object TestData {

  // testing data
  case class User(name: String, age: Int)

  def users(num: Int = 1000): Vector[User] =
    (0 to num)
      .map(i => User(s"user-$i", i % 100))
      .toVector

  case class Animal(
    name: String,
    animalType: String,
    whenLived: String,
    sizeInMeters: Double,
    whenDiscovered: Int,
    location: String,
    color: Option[String],
    food: String
  )

  // property-based generator for Animal rows, see AnimalsGenTest
  val animalGen: Gen[Animal] =
    for {
      name <- Gen.oneOf(
        "Tyrannosaurus rex",
        "Triceratops horridus",
        "Stegosaurus stenops",
        "Brachiosaurus altithorax",
        "Spinosaurus aegyptiacus",
        "Diplodocus carnegii",
        "Velociraptor mongoliensis",
        "Ankylosaurus magniventris",
        "Parasaurolophus walkeri",
        "Iguanodon bernissartensis",
        "Allosaurus fragilis",
        "Deinonychus antirrhopus",
        "Pachycephalosaurus wyomingensis",
        "Compsognathus longipes",
        "Archaeopteryx lithographica"
      )
      animalType <- Gen.oneOf(
        "Dinosaur - Theropod",
        "Dinosaur - Sauropod",
        "Dinosaur - Ceratopsian",
        "Dinosaur - Ornithopod",
        "Dinosaur - Stegosaur",
        "Dinosaur - Ankylosaur",
        "Dinosaur - Hadrosaur",
        "Dinosaur - Sauropodomorph",
        "Synapsid - Cynodont",
        "Synapsid - Dicynodont",
        "Synapsid - Mammal",
        "Archosauriform",
        "Archosauromorph"
      )
      whenLived <- Gen.oneOf(
        "Late Triassic",
        "Middle Triassic",
        "Early Jurassic",
        "Late Jurassic",
        "Early Cretaceous",
        "Late Cretaceous"
      )
      sizeInMeters <- Gen.choose(0.2, 30.0)
      whenDiscovered <- Gen.choose(1820, 2025)
      location <- Gen.oneOf(
        "Montana, USA",
        "Colorado, USA",
        "Wyoming, USA",
        "Alberta, Canada",
        "Liaoning, China",
        "Bavaria, Germany",
        "Patagonia, Argentina",
        "Bahariya Oasis, Egypt",
        "Isle of Wight, UK"
      )
      color <- Gen.oneOf(
        None,
        Some("brown"),
        Some("green"),
        Some("gray"),
        Some("reddish-brown"),
        Some("dark green")
      )
      food <- Gen.oneOf(
        "Carnivore",
        "Herbivore",
        "Omnivore",
        "Piscivore",
        "Insectivore",
        "Herbivore/Omnivore",
        "Carnivore/Omnivore"
      )
    } yield Animal(
      name,
      animalType,
      whenLived,
      sizeInMeters,
      whenDiscovered,
      location,
      color,
      food
    )

  def animals(num: Int = 300): Vector[Animal] =
    LazyList
      .continually(Gen.listOfN(num, animalGen).sample)
      .flatten
      .head
      .toVector

}
