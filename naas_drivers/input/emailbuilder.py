from naas_drivers.driver import InDriver
from htmlBuilder import tags, attributes
import IPython.core.display
import pandas as pd
import requests
import uuid
import os
import warnings

#  https://litmus.com/community/templates/31-accessible-product-announcement-email
# https://github.com/rodriguezcommaj/accessible-emails
base_style = """
/* CLIENT-SPECIFIC STYLES */
body, table, td, a { -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }
table, td { mso-table-lspace: 0pt; mso-table-rspace: 0pt; }
img { -ms-interpolation-mode: bicubic; }

/* RESET STYLES */
img { border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }
table { border-collapse: collapse !important; text-align: left !important; }
body { height: 100% !important; margin: 0 !important; padding: 0 !important; width: 100% !important; }

/* iOS BLUE LINKS */
a[x-apple-data-detectors] {
    color: inherit !important;
    text-decoration: none !important;
    font-size: inherit !important;
    font-family: inherit !important;
    font-weight: inherit !important;
    line-height: inherit !important;
}

/* GMAIL BLUE LINKS */
u + #body a {
    color: inherit;
    text-decoration: none;
    font-size: inherit;
    font-family: inherit;
    font-weight: inherit;
    line-height: inherit;
}

/* SAMSUNG MAIL BLUE LINKS */
#MessageViewBody a {
    color: inherit;
    text-decoration: none;
    font-size: inherit;
    font-family: inherit;
    font-weight: inherit;
    line-height: inherit;
}

a { color: #B200FD; font-weight: 600; text-decoration: underline; }
a:hover { color: #000000 !important; text-decoration: none !important; background-color: #5c1958 !important; }
a.button:hover { color: #ffffff !important; background-color: #5c1958 !important; }

td, th {
    padding: 10px;
}

table * {
    margin: 18px 0 !important;
}

.table_border {
  border-collapse: collapse;
  border-radius: 1em;
  overflow: hidden;
}
.table_border tr:hover {
  background-color: AliceBlue !important;
  color: black;
}
.table_border tr:first-child td:first-of-type {
  border-top-left-radius: 10px;
}
.table_border tr:first-child td:last-of-type {
  border-top-right-radius: 10px;
}

.table_border tr:last-of-type td:first-of-type {
  border-bottom-left-radius: 10px;
}
.table_border tr:last-of-type td:last-of-type {
  border-bottom-right-radius: 10px;
}
.table_border tr:nth-child(even) { background-color: ghostwhite}

@media screen and (min-width:600px) {
    h1 { font-size: 48px !important; line-height: 48px !important; }
    .intro { font-size: 24px !important; line-height: 36px !important; }
}
.basic_font {
    font-family: 'Avenir Next', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol';  # noqa: E501
}
"""

table_ie9 = """
<!--[if (gte mso 9)|(IE)]>
<table cellspacing="0" cellpadding="0" border="0" width="720" align="center" role="presentation"><tr><td>
<![endif]-->
"""

table_ie9_close = """
<!--[if (gte mso 9)|(IE)]>
</td></tr></table>
<![endif]-->
"""


class EmailBuilder(InDriver):
    """ EmailBuilder generator lib"""

    deprecated = False

    def __init__(self, deprecated=False):
        self.deprecated = deprecated

    def deprecatedPrint(self):
        # TODO remove this in june 2021
        if self.deprecated:
            warnings.warn(
                "[Warning], naas_drivers.html is deprecated,\n use naas_drivers.emailBuilder instead it will be remove in 1 june 2021"
            )

    def __align(self, mode):
        margin = "0 auto 0 0"
        if mode == "right":
            margin = "0 0 0 auto"
        if mode == "center":
            margin = "0 auto 0 auto"
        return margin

    def __convert(self, data, name):
        args = [data]
        if name.find("_") > -1:
            args_list = name.split("_")[1:]
            args_list = list(filter(lambda x: not x.isdigit(), args_list))
            args.extend(args_list)
        if name.startswith("image") and isinstance(data, str):
            return self.image(*args)
        if name.startswith("logo") and isinstance(data, str):
            return self.logo(*args)
        elif name.startswith("link") and isinstance(data, str):
            return self.link(*args)
        elif name.startswith("button") and isinstance(data, str):
            return self.button(*args)
        elif name.startswith("info") and isinstance(data, str):
            return self.button(*args)
        elif name.startswith("title") and isinstance(data, str):
            return self.title(*args)
        elif name.startswith("heading") and isinstance(data, str):
            return self.heading(*args)
        elif name.startswith("address") and isinstance(data, str):
            return self.address(*args)
        elif name.startswith("table") and (
            isinstance(data, pd.DataFrame) or isinstance(data, list)
        ):
            return self.table(*args)
        elif name.startswith("subheading") and isinstance(data, str):
            return self.subheading(*args)
        else:
            return self.text(*args) if isinstance(data, str) else data

    def address(self, title, content):
        self.deprecatedPrint()
        return tags.Address(
            attributes.InlineStyle(
                font_size="16px",
                font_style="normal",
                font_weight="400",
                line_height="24px",
            ),
            tags.Strong(tags.Text(title)),
            tags.Text(content),
        )

    def link(self, link, title="Open", color="#B200FD"):
        self.deprecatedPrint()
        return tags.A(
            attributes.Href(link),
            attributes.InlineStyle(color=color, text_decoration="underline"),
            tags.Text(title),
        )

    def button(
        self,
        link,
        text="Open",
        width="auto",
        color="white",
        background_color="black",
    ):
        self.deprecatedPrint()
        return tags.Center(
            tags.Div(
                attributes.InlineStyle(margin="48px 0"),
                tags.A(
                    attributes.Class("button"),
                    attributes.Href(link),
                    attributes.InlineStyle(
                        background_color=background_color,
                        color=color,
                        border_radius="4px",
                        display="inline-block",
                        font_family="sans-serif",
                        font_size="18px",
                        font_weight="bold",
                        line_height="60px",
                        text_align="center",
                        text_decoration="none",
                        width=width,
                        max_width="300px",
                        padding_left="10px",
                        padding_right="10px",
                        _webkit_text_size_adjust="none",
                    ),
                    tags.Text(text),
                ),
            )
        )

    def info(self, *elems):
        self.deprecatedPrint()
        return tags.Div(
            attributes.InlineStyle(
                background_color="ghostwhite",
                border_radius="4px",
                padding="24px 48px",
            ),
            *elems,
        )

    def space(self):
        self.deprecatedPrint()
        return tags.Br()

    def separator(self):
        self.deprecatedPrint()
        return tags.hr()

    def table(self, data, border=True):
        self.deprecatedPrint()
        elems = []
        table_arr = None
        row_link = False
        row_link_index = -1
        if isinstance(data, pd.DataFrame):
            table_arr = list(data.values.tolist())
            row_link = True if "row_link" in data.columns else False
            try:
                row_link_index = list(data.columns).index("row_link")
            except ValueError:
                pass
        elif isinstance(data, list):
            table_arr = data
        else:
            error_text = f"Table should be array, not {data.__name__}"
            self.print_error(error_text)
            return None
        for row in table_arr:
            res = []
            if isinstance(row, list):
                for i in range(len(row)):
                    cell = row[i]
                    if isinstance(data, pd.DataFrame):
                        col = list(data.columns)[i]
                        if row_link:
                            if col != "row_link":
                                link = row[row_link_index]
                                res.append(
                                    tags.Td(
                                        tags.A(
                                            attributes.Href(link),
                                            self.__convert(cell, col),
                                        )
                                    )
                                )
                        else:
                            res.append(tags.Td(self.__convert(cell, col)))
                    else:
                        res.append(
                            tags.Td(tags.Text(cell) if isinstance(cell, str) else cell)
                        )
            else:
                res.append(tags.Td(tags.Text(row) if isinstance(row, str) else row))
            elems.append(tags.Tr(res))
        tab = tags.Table(
            attributes.InlineStyle(width="100%"),
            attributes.Class("table_border") if border else None,
            elems,
        )
        return tags.P(tab)

    def logo(self, src, link=None, name="Logo", align="center", size="80px"):
        self.deprecatedPrint()
        return self.image(src, link, name, width=size, height=size, align=align)

    def image(
        self, src, link=None, name="Cover", align="left", width="100%", height="80%"
    ):
        self.deprecatedPrint()
        if src is None:
            return None
        elems_img = [
            attributes.Src(f"{src}?naas_uid={str(uuid.uuid4())}"),
            attributes.Height(height),
            attributes.Width(width),
            {"name": "border", "value": 0},
            attributes.InlineStyle(
                border_radius="4px",
                margin=self.__align(align),
                display="block",
            ),
            {"name": "alt", "value": name},
        ]
        if link:
            return tags.A(attributes.Href(link), tags.Img(elems_img))
        else:
            return tags.Img(elems_img)

    def title(self, title, heading=None):
        self.deprecatedPrint()
        return tags.H1(
            attributes.InlineStyle(
                color="#000000",
                font_size="32px",
                font_weight="800",
                line_height="32px",
                margin="48px 0",
                text_align="center",
            ),
            tags.Text(title),
            tags.Br(),
            (
                tags.Span(
                    attributes.InlineStyle(
                        font_size="24px", font_weight="600", color="darkgray"
                    ),
                    tags.Text(heading),
                )
                if heading
                else None
            ),
        )

    def heading(self, text):
        self.deprecatedPrint()
        return tags.H2(
            attributes.InlineStyle(
                color="#000000",
                font_size="28px",
                font_weight="600",
                line_height="32px",
                margin="48px 0 24px 0",
                text_align="center",
            ),
            tags.Text(text),
        )

    def subheading(self, text):
        self.deprecatedPrint()
        return self.text(text, font_size="24px")

    def text(self, text, font_size="18px"):
        self.deprecatedPrint()
        return tags.P(
            tags.Text(text),
            attributes.InlineStyle(
                font_size=font_size, padding_left="10px", padding_right="10px"
            ),
        )

    def header(self, *elems):
        self.deprecatedPrint()
        return tags.Header(*elems)

    def footer(self, text, first=None, *elems):
        self.deprecatedPrint()
        one = [
            attributes.InlineStyle(
                font_size="16px",
                font_weight="400",
                line_height="24px",
                margin_top="48px",
            ),
            tags.Text(text),
            first,
        ]
        return tags.Footer(tags.P(one), *elems)

    def main(
        self,
        **kwargs,
    ):
        self.deprecatedPrint()
        items = []
        for key, value in kwargs.items():
            items.append(self.__convert(value, key))
        return [tags.Main(items)]

    def __display(self, content, mode):
        uid = uuid.uuid4().hex
        if mode is None:
            return
        if mode == "shadow":
            shadow = f"""
            <script type="text/javascript">
            var div_{uid} = document.createElement('div');
            var shadowRoot_{uid} = div_{uid}.attachShadow({{mode: 'open'}});
            shadowRoot_{uid}.innerHTML = `
            {content}
            `;
            console.log('injected')
            </script>
            """
            IPython.core.display.display(IPython.core.display.HTML(shadow))
        elif mode == "iframe":
            iframe = f"""
            <script>
                function resizeIframe_{uid}(obj) {{
                    obj.style.height = obj.contentWindow.document.documentElement.scrollHeight + 'px';
                }}
            </script>
            <iframe id="FileFrame_{uid}" src="about:blank"
                style="border: 0; width: 100%; height: 100%" onload="resizeIframe_{uid}(this)">
            </iframe>
            <script type="text/javascript">
            var doc_{uid} = document.getElementById('FileFrame_{uid}').contentWindow.document;
            doc_{uid}.open();
            doc_{uid}.write(`{content}`);
            doc_{uid}.close();
            </script>
            """
            IPython.core.display.display(IPython.core.display.HTML(iframe))
        elif mode == "embed":
            IPython.core.display.display(IPython.core.display.HTML(content))

    def __export(self, html, filename, css=""):
        result = html.replace("</head>", f'<style id="naas_css">{css}</style></head>')
        extension = filename.split(".")[1]
        output = None
        json = {
            "html": result,
            "emulateScreenMedia": True,
            "ignoreHttpsErrors": True,
            "scrollPage": False,
            "screenshot": {"type": extension},
        }
        if filename.endswith(".png") or filename.endswith(".jpeg"):
            output = "screenshot"
            json["screenshot"] = {"type": extension}
        elif filename.endswith(".pdf"):
            output = "pdf"
            json["pdf"] = {"width": "20.5cm", "height": "36.5cm"}
        else:
            error_text = f"extension {filename.split('.')[1]} not suported for now"
            self.print_error(error_text)
        json["output"] = output
        req = requests.post(
            url=f"{os.environ.get('SCREENSHOT_API', 'http://naas-screenshot:9000')}/api/render",
            json=json,
        )
        req.raise_for_status()
        open(filename, "wb").write(req.content)
        print(f"Saved as {filename}")

    def export(self, html, filenames, css=None):
        """ create html export and add css to it"""
        self.deprecatedPrint()
        if isinstance(filenames, list):
            for filename in filenames:
                self.__export(html, filename, css)
        else:
            self.__export(html, filenames, css)

    def generate(self, title, logo=None, display="embed", footer=None, **kwargs):
        self.deprecatedPrint()
        gen_html = tags.Html(
            attributes.Lang("en"),
            tags.Head(
                tags.Meta(
                    attributes.HttpEquiv("Content-Type"),
                    attributes.Content("text/html; charset=utf-8"),
                ),
                tags.Meta(
                    attributes.HttpEquiv("Content-Type"),
                    attributes.Content("width=device-width, initial-scale=1"),
                ),
                tags.Meta(
                    attributes.Name("viewport"),
                    attributes.Content("width=device-width, initial-scale=1"),
                ),
                tags.Meta(
                    attributes.HttpEquiv("X-UA-Compatible"),
                    attributes.Content("IE=edge"),
                ),
                tags.Style(tags.Text(base_style)),
                tags.Title(tags.Text(title)),
            ),
            tags.Body(
                attributes.InlineStyle(margin="0 !important", padding="0 !important"),
                tags.Div(
                    attributes.InlineStyle(
                        display="none", max_height="0", overflow="hidden"
                    ),
                    tags.Text(title),
                ),
                tags.Div(
                    attributes.InlineStyle(
                        display="none", max_height="0", overflow="hidden"
                    ),
                    tags.Text("&nbsp;‌" * 240),
                ),
                tags.Text(table_ie9),
                tags.Div(
                    {"name": "role", "value": "article"},
                    {"name": "aria-label", "value": title},
                    attributes.Lang("en"),
                    attributes.Class("basic_font"),
                    attributes.InlineStyle(
                        background_color="white",
                        color="#2b2b2b",
                        font_size="18px",
                        font_weight="400",
                        line_height="28px",
                        margin="0 auto",
                        max_width="720px",
                        padding="40px 20px 40px 20px",
                    ),
                    self.header(logo, self.title(title)),
                    self.main(**kwargs),
                    footer,
                ),
                tags.Text(table_ie9_close),
            ),
        )
        res = gen_html.render()
        self.__display(res, display)
        return res
