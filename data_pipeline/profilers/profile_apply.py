import cProfile

import data_pipeline.utils.args as args
import data_pipeline.apply as apply
import data_pipeline.constants.const as const


cProfile.run("apply.main()", "apply.prof")
