package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class SimpsonsSparkProcessTest extends ContextProvider{

  "when i execute runProcess with a correct RuntimeContext" should "Return 0" in {
     val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(config)
     val simpsonsSparkProcess: SimpsonsSparkProcess = new SimpsonsSparkProcess

    val exitCode: Int = simpsonsSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe 0
  }

  it should "return -1 when any error is thrown" in {
    val fakeRuntimeContext: FakeRuntimeContext = new FakeRuntimeContext(configError)
    val simpsonsSparkProcess: SimpsonsSparkProcess = new SimpsonsSparkProcess

    val exitCode: Int = simpsonsSparkProcess.runProcess(fakeRuntimeContext)

    exitCode shouldBe -1
  }


}
