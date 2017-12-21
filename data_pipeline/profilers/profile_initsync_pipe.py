import cProfile

import data_pipeline.utils.args as args
import data_pipeline.initsync_pipe as initsync_pipe
import data_pipeline.constants.const as const


cProfile.run("initsync_pipe.main()", "initsync_pipe.prof")
