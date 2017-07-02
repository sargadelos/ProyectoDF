addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")

fullClasspath in Runtime := (fullClasspath in Compile).value
