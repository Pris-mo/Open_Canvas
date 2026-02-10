import sys

print("python:", sys.executable)

import orchestrator
print("orchestrator:", orchestrator.__file__)

import chunker
print("chunker:", chunker.__file__)

import canvas_crawler.canvascrawler
print("canvas_crawler.canvascrawler:", canvas_crawler.canvascrawler.__file__)

import pre_processer.fileConversion
print("pre_processer.fileConversion:", pre_processer.fileConversion.__file__)
