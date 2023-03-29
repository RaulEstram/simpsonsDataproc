package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.datio.dataproc.sdk.launcher.SparkLauncher

class SimpsonsLauncherTest extends ContextProvider {

  "SparkLauncher execute" should "Return 0 in success execution" in {
    val args: Array[String] = Array("src/test/resources/config/simpsonsConfigTest.conf", "SimpsonsSparkProcess")
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 0
  }

  it should "return 1 when Fatal dataproc error is thrown" in {
    val args: Array[String] = Array("src/test/resources/config/simpsonsErrorLauncherConfigTest.conf", "SimpsonsSparkProcess")
    val exitCode: Int = new SparkLauncher().execute(args)

    exitCode shouldBe 1
  }


}
