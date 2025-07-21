import argparse
import logging
import sys
from canvascrawler.client import Canvas
from canvascrawler.storage import StorageManager
from canvascrawler.crawler import CanvasCrawler
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

    
    if not args.token:
        logger.error("No API token provided. Use --token or set CANVAS_TOKEN.")
        sys.exit(1)

    # Initialize Canvas API client
    client = Canvas(token=args.token, url="https://learn.canvas.net")
    logger.debug("Canvas client initialized")

    # Set up file storage
    storage = StorageManager(args.output_dir, logger)
    logger.debug(f"Storage initialized at {args.output_dir}")

    # Create crawler
    crawler = CanvasCrawler(
        client=client,
        course_id=args.course_id,
        storage=storage,
        depth_limit=args.depth_limit,
        logger=logger,
    )

    # Run the crawler
    crawler.run()

    logger.info("Finished crawling (seed-only mode)")
    sys.exit(0)


if __name__ == '__main__':
    main()