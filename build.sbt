name := "MemcachedClient"

organization := "org.hydra"

version := "0.0.1"

crossScalaVersions := Seq("2.9.1", "2.9.0", "2.8.1", "2.8.0")

libraryDependencies <++= scalaVersion { scalaVersion =>
  // The dependencies with proper scope
  Seq(
//:    "junit"          % "junit"           % "4.8.2"  % "test",
    "com.novocode"   % "junit-interface" % "0.7"  % "test"
  )
}

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-Xcheckinit")

