import sys

print("python:", sys.executable)

import orchestrator
print("orchestrator:", orchestrator.__file__)

import chunker
print("chunker:", chunker.__file__)

import canvas_crawler.canvascrawler
print("canvas_crawler.canvascrawler:", canvas_crawler.canvascrawler.__file__)

import fileConversion
print("fileConversion:", fileConversion.__file__)
