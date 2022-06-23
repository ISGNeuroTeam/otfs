name := "OTFS"
description := "Feature Store for OT.Platform"
version := "1.0.2"
scalaVersion := "2.11.12"
crossPaths := false

credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  sys.env.getOrElse("NEXUS_HOSTNAME", ""),
  sys.env.getOrElse("NEXUS_COMMON_CREDS_USR", ""),
  sys.env.getOrElse("NEXUS_COMMON_CREDS_PSW", "")
)

resolvers +=
  ("Sonatype OSS Snapshots" at "http://s.dev.isgneuro.com/repository/ot.platform-sbt-releases/")
    .withAllowInsecureProtocol(true)

val dependencies = new {
  private val dispatcherSdkVersion = "1.2.0"
  //  private val configVersion = "1.3.4"
  //  private val scalatestVersion = "3.2.9"

  val dispatcherSdk = "ot.dispatcher" % "dispatcher-sdk_2.11" % dispatcherSdkVersion  % Compile
  //  val config = "com.typesafe" % "config" % configVersion % Compile
  //  val scalatest = "org.scalatest" %% "scalatest" % scalatestVersion % Test
}

libraryDependencies ++= Seq(
  dependencies.dispatcherSdk,
  //  dependencies.config,
  //  dependencies.scalatest
)

Test / parallelExecution := false

Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.aFSInitTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSBranchTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSPutTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSGetTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSMergeTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSDelBranchTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSDelModelTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSGetBranchesTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSGetParentBranchTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.FSGetChildBranchesTest") )
Test / testOptions += Tests.Setup( loader => loader.loadClass("com.isgneuro.otp.plugins.fs.commands.RepartitionTest") )

publishTo := Some(
  "Sonatype Nexus Repository Manager" at sys.env.getOrElse("NEXUS_OTP_URL_HTTPS", "")
    + "/repository/ot.platform-sbt-releases"


)

//Seq(Tests.Filter(t => definedTests))
/*testGrouping := {definedTests in Test map { tests =>
  tests.map { test =>
    import Tests._
    new Group(
      name = test.name,
      tests = Seq(test),
      runPolicy = InProcess)
  }.sortWith(_.name < _.name)
}}*/
