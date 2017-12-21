import cProfile

import data_pipeline.utils.args as args
import data_pipeline.extract as extract
import data_pipeline.constants.const as const


cProfile.run("extract.main()", "extract.prof")
