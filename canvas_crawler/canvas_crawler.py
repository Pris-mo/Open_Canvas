import argparse
import logging
import sys
from canvascrawler.client import Canvas, WebClient
from canvascrawler.storage import StorageManager
from canvascrawler.crawler import CanvasCrawler
from canvascrawler.handlers import ClientBundle
import os
from datetime import datetime

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
    parser.add_argument('--canvas-url', default='https://learn.canvas.net', help='Base URL of the Canvas instance')
    parser.add_argument('--verbose', action='store_true', help='Enable debug logging')
    return parser.parse_args()

def setup_logging(verbose: bool, log_path="crawler.log"):
    level = logging.DEBUG if verbose else logging.INFO

    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    logger = logging.getLogger("canvas_crawler")
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    # File handler
    fh = logging.FileHandler(log_path)
    fh.setLevel(level)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger


def main():
    args = parse_args()
    
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(args.output_dir,"logs", f"crawl_{ts}.log")
    logger = setup_logging(args.verbose, log_path=log_path)
    logger.info("Starting Canvas Crawler")
    logger.debug(f"Args: {args}")

    args.output_dir = os.path.join(args.output_dir,"output", str(args.course_id))
    
    if not args.token:
        logger.error("No API token provided. Use --token or set CANVAS_TOKEN.")
        sys.exit(1)

    # Initialize Canvas API client
    canvas = Canvas(token=args.token, url=args.canvas_url)
    
    # Initialize Web client
    web_client = WebClient(timeout=30)

    # Bundle clients
    client = ClientBundle(canvas=canvas, web=web_client)

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