addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")

addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.3.15")

addSbtPlugin("ch.epfl.scala" % "sbt-version-policy" % "3.2.1")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.8")

// Signs artifacts for Maven Central (`publishSigned`; key from the gpg keyring).
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.1")
