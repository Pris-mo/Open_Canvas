import sys

print("python:", sys.executable)

import yaml
import markitdown

import orchestrator
import chunker
import canvas_crawler.canvascrawler
import pre_processer.fileConversion

print("OK: full pipeline imports succeeded")
