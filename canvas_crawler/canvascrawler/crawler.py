from collections import deque
from .handlers import HandlerFactory

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
            ("modules",           {"course_id": self.course_id, "item_id": None, "depth": 0}),
#            ("announcements",     {"course_id": self.course_id, "item_id": None, "depth": 0}),
#            ("assignments", {"course_id": self.course_id, "item_id": None, "depth": 0}),
        ]


# No handler for subheader, I want to skip these, i.e these can be ignored


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
                handler.run(context)
            except Exception as e:
                self.logger.error(f"Failed to handle {content_type}/{context['item_id']}: {e}")
                continue

            # discover new links via parsed JSON or client calls...
            for link_type, new_context in self.discover_links(content_type, context):
                queue.append((link_type, new_context))

    def discover_links(self, content_type, context):
        cid        = context["course_id"]
        next_depth = context["depth"] + 1
        links      = []

        # 1) modules list → each module
        if content_type == "modules":
            for mod in self.client.get_modules(cid):
                links.append((
                    "module",
                    {"course_id": cid, "item_id": mod["id"], "depth": next_depth}
                ))

        # 2) one module → its items (pages, assignments, files, etc.)
        elif content_type == "module":
            for mi in self.client.get_module_items(cid, context["item_id"]):
                ct = mi["type"].lower()  # e.g. "page", "assignment", "file"
                
                if ct == "subheader":
                    continue
                elif ct == "page":
                    links.append((
                        ct,
                        {"course_id": cid, "item_id": mi["page_url"], "depth": next_depth}
                    ))
                elif ct == "discussion":
                    links.append((
                        ct,
                        {"course_id": cid, "item_id": mi["content_id"], "depth": next_depth}
                    ))
                else:
                    links.append((
                        ct,
                        {"course_id": cid, "item_id": mi["id"], "depth": next_depth}
                    ))

        # 3) assignments list → each assignment
        elif content_type == "assignments":
            for a in self.client.get_assignments(cid):
                links.append((
                    "assignment",
                    {"course_id": cid, "item_id": a["id"], "depth": next_depth}
                ))

        # 4) pages list → each page
        elif content_type == "pages":
            for p in self.client.get_pages(cid):
                links.append((
                    "page",
                    {"course_id": cid, "item_id": p["id"], "depth": next_depth}
                ))

        # 5) announcements list → each announcement
        elif content_type == "announcements":
            for ann in self.client.get_announcements(cid):
                links.append((
                    "announcement",
                    {"course_id": cid, "item_id": ann["id"], "depth": next_depth}
                ))

        # return all the new work items
        return links
