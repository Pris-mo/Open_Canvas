from collections import deque
from .handlers import HandlerFactory
from .utils import extract_hrefs, classify_link

class CanvasCrawler:
    def __init__(self, client, course_id, storage, depth_limit, logger):
        self.client      = client
        self.course_id   = course_id
        self.storage     = storage
        self.depth_limit = depth_limit
        self.logger      = logger


    def _seed(self):
         # each context dict carries course_id, item_id, and current depth
        return [
#            ("syllabus",          {"course_id": self.course_id, "item_id": None, "depth": 0}),
#            ("modules",           {"course_id": self.course_id, "item_id": None, "depth": 0}),
            ("announcements",     {"course_id": self.course_id, "item_id": None, "depth": 0}),
#            ("assignments", {"course_id": self.course_id, "item_id": None, "depth": 0}),
        ]

    def _enqueue(self, queue, content_type, context, source="unknown"):
        # Enforce: if we can discover/queue it, we must be able to handle it
        if not HandlerFactory.has_handler(content_type):
            self.logger.error(
                f"Discovered/queued content_type='{content_type}' from {source} "
                f"but no handler exists. context={context}"
            )
            return False

        queue.append((content_type, context))
        return True

    def run(self):
        # seed with syllabus, modules, announcements, etc.
        queue = deque(self._seed())
        seen = set()

        while queue:
            content_type, context = queue.popleft()
            if context["depth"] > self.depth_limit:
                continue

            key = (content_type, context["item_id"])
            if key in seen:
                continue
            seen.add(key)

            try:
                handler = HandlerFactory.get_handler(content_type, self.client, self.storage, self.logger)

                # inject content_type so handlers can reference it (esp. for locked stubs)
                ctx = dict(context)
                ctx["content_type"] = content_type

                parsed = handler.run(ctx)
                if not parsed:
                    continue

                # Special case: skip external links from further processing (We don't want to crawl the web!)
                if content_type == "external_link":
                    continue
            except Exception as e:
                self.logger.error(f"Failed to handle {content_type}/{context['item_id']}: {e}")
                continue

            # Enqueue links discovered via the module/assignment logic
            for link_type, new_context in self.discover_links(content_type, context):
                self._enqueue(queue, link_type, new_context, source=f"discover_links:{content_type}")
            
            # Enqueue links via href extraction
            body_html = parsed.get("body", "")
            base_url = self.client.canvas.server_url.rstrip("/")
            next_depth = context["depth"] + 1

            for href in extract_hrefs(body_html):
                classified = classify_link(href, base_url)
                if classified:
                    ct, item_id = classified
                    new_ctx = {
                        "course_id": context["course_id"],
                        "item_id":   item_id,
                        "depth":     next_depth
                    }
                    self._enqueue(queue, ct, new_ctx, source="href_extraction")
                else:
                    if href.startswith("http://") or href.startswith("https://"):
                        self._enqueue(queue, "external_link", {
                            "course_id": context["course_id"],
                            "item_id": href,
                            "depth": next_depth
                        }, source="href_external")
                    pass


    def discover_links(self, content_type, context):
        cid        = context["course_id"]
        next_depth = context["depth"] + 1
        links      = []

        # 1) modules list -> each module
        if content_type == "modules":
            for mod in self.client.canvas.get_modules(cid):
                links.append((
                    "module",
                    {"course_id": cid, "item_id": mod["id"], "depth": next_depth}
                ))

        # 2) one module -> its items (pages, assignments, files, etc.)
        elif content_type == "module":
            for mi in self.client.canvas.get_module_items(cid, context["item_id"]):
                ct = mi["type"].lower()  # e.g. "page", "assignment", "file"
                
                # NOTE: Canvas module items use different IDs depending on type.
                # We must translate module-item records into real content IDs here.
                if ct == "subheader":
                    continue
                elif ct == "page":
                    item_id = mi["page_url"]
                elif ct == "assignment" and mi.get("quiz_lti") == True:
                    # Canvas New Quizzes appear as module items of type "Assignment" with quiz_lti=True.
                    # They cannot be fetched/parsed via the normal Assignments API, so route them to a
                    # dedicated handler.
                    ct = "new_quiz"
                    item_id = mi["content_id"]
                elif ct in ("assignment", "discussion","quiz"):
                    item_id = mi["content_id"]
                elif ct in ("externalurl", "externaltool"):
                    ct = "external_link"
                    item_id = mi.get("external_url") or mi.get("url")
                    if not item_id:
                        self.logger.warning(f"Module item external link missing URL: {mi}")
                        continue
                else:
                    # Fallback: module item id (may not always map to real object)
                    item_id = mi["id"]

                links.append((
                    ct,
                    {"course_id": cid, "item_id": item_id, "depth": next_depth}
                ))

        # 3) assignments list -> each assignment
        elif content_type == "assignments":
            for a in self.client.canvas.get_assignments(cid):
                links.append((
                    "assignment",
                    {"course_id": cid, "item_id": a["id"], "depth": next_depth}
                ))

        # 4) pages list -> each page
        elif content_type == "pages":
            for p in self.client.canvas.get_pages(cid):
                links.append((
                    "page",
                    {"course_id": cid, "item_id": p["id"], "depth": next_depth}
                ))

        # 5) announcements list -> each announcement
        elif content_type == "announcements":
            for ann in self.client.canvas.get_announcements(cid):
                links.append((
                    "announcement",
                    {"course_id": cid, "item_id": ann["id"], "depth": next_depth}
                ))
            


        # return all the new work items
        return links
