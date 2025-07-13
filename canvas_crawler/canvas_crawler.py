import argparse
import logging
import sys
from canvascrawler.client import Canvas
import os

def parse_args():
    parser = argparse.ArgumentParser(
        description="Canvas Crawler: extract JSON + raw files from Canvas LMS"
    )
    parser.add_argument('--course-id', required=True, help='Canvas course ID')
    parser.add_argument(
        '--token',
        default=os.environ.get("CANVAS_TOKEN"),
        required=False,
        help='Canvas API access token (or set CANVAS_TOKEN env variable)'
    )
    parser.add_argument('--output-dir', default='./output', help='Output directory')
    parser.add_argument('--depth-limit', type=int, default=15, help='Max recursion depth')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('canvas_crawler')

def main():
    args = parse_args()
    logger = setup_logging(args.verbose)
    logger.info("Starting Canvas Crawler")
    logger.debug(f"Args: {args}")

    # Initialize Canvas API client
    client = Canvas(token=args.token, url="https://learn.canvas.net")
    logger.debug("Canvas client initialized")

    # TODO: instantiate crawler, storage, and kick off crawl
    # from canvascrawler.crawler import CanvasCrawler
    # crawler = CanvasCrawler(client, args.course_id, args.output_dir, args.depth_limit, logger)
    # crawler.run()

    logger.info("Finished (stub)")

if __name__ == '__main__':
    main()