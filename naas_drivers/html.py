from htmlBuilder import tags, attributes
import IPython.core.display
import pandas as pd
import uuid

#  https://litmus.com/community/templates/31-accessible-product-announcement-email
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


.table_border td, th {
    padding: 8px;
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
"""

basic_font = "'Avenir Next', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif, 'Apple Color Emoji', 'Segoe UI Emoji', 'Segoe UI Symbol'"  # noqa: E501
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


class Html:
    """ HTML generator lib"""

    def address(self, title, content):
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
        return tags.A(
            attributes.Href(link),
            attributes.InlineStyle(color=color, text_decoration="underline"),
            tags.Text(title),
        )

    def header(self, *elems):
        return tags.Header(*elems)

    def footer(self, text, first=None, *elems):
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

    def info(self, *elems):
        return tags.Div(
            attributes.InlineStyle(
                background_color="ghostwhite",
                border_radius="4px",
                padding="24px 48px",
            ),
            *elems,
        )

    def space(self):
        return tags.Br()

    def __convert(self, data, name):
        if name.startswith("image") and isinstance(data, str):
            return self.image(data)
        if name.startswith("logo") and isinstance(data, str):
            return self.image(data, width="80px", height="80px")
        elif name.startswith("link") and isinstance(data, str):
            return self.link(data)
        elif name.startswith("button") and isinstance(data, str):
            return self.button(data)
        elif name.startswith("info") and isinstance(data, str):
            return self.button(data)
        elif name.startswith("title") and isinstance(data, str):
            return self.title(data)
        elif name.startswith("subtitle") and isinstance(data, str):
            return self.subtitle(data)
        elif name.startswith("table") and (
            isinstance(data, pd.DataFrame) or isinstance(data, list)
        ):
            return self.table(data)
        elif name.startswith("heading") and isinstance(data, str):
            return self.text(data, font_size="24px")
        else:
            return self.text(data) if isinstance(data, str) else data

    def table(self, data, border=True):
        elems = []
        table_arr = None
        if isinstance(data, pd.DataFrame):
            table_arr = list(data.values.tolist())
        elif isinstance(data, list):
            table_arr = data
        else:
            raise ValueError("Table should be array")
        for row in table_arr:
            res = []
            if isinstance(row, list):
                for i in range(len(row)):
                    cell = row[i]
                    if isinstance(data, pd.DataFrame):
                        col = list(data.columns)[i]
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

    def logo(self, src, link=None, name="Logo", size="80px"):
        return self.image(src, link, name, width=size, height=size)

    def image(self, src, link=None, name="Cover", width="100%", height="80%"):
        if src is None:
            return None
        elems_img = [
            attributes.Src(f"{src}?naas_uid={str(uuid.uuid4())}"),
            attributes.Height(height),
            attributes.Width(width),
            {"name": "border", "value": 0},
            attributes.InlineStyle(
                border_radius="4px",
                display="block",
            ),
            {"name": "alt", "value": name},
        ]
        if link:
            return tags.A(attributes.Href(link), tags.Img(elems_img))
        else:
            return tags.Img(elems_img)

    def title(self, title, subtitle=None):
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
                    tags.Text(subtitle),
                )
                if subtitle
                else None
            ),
        )

    def subtitle(self, subtitle):
        return tags.H2(
            attributes.InlineStyle(
                color="#000000",
                font_size="28px",
                font_weight="600",
                line_height="32px",
                margin="48px 0 24px 0",
                text_align="center",
            ),
            tags.Text(subtitle),
        )

    def button(
        self,
        link,
        text="Check it",
        color="white",
        width="auto",
        background_color="black",
    ):
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

    def text(self, text, font_size="18px"):
        return tags.P(tags.Text(text), attributes.InlineStyle(font_size=font_size))

    def main(
        self,
        **kwargs,
    ):
        items = []
        for key, value in kwargs.items():
            items.append(self.__convert(value, key))
        return [tags.Main(items)]

    def generate(self, title, logo=None, display=True, footer=None, **kwargs):
        html = tags.Html(
            attributes.Lang("en"),
            tags.Head(
                tags.Title(tags.Text(title)),
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
            ),
            tags.Body(
                attributes.InlineStyle(margin="0 !important", padding="0 !important"),
                tags.Div(
                    attributes.InlineStyle(
                        display="none", max_height="0", overflow="hidden"
                    )
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
                    attributes.InlineStyle(
                        background_color="white",
                        color="#2b2b2b",
                        font_size="18px",
                        font_weight="400",
                        line_height="28px",
                        margin="0 auto",
                        max_width="720px",
                        padding="40px 20px 40px 20px",
                        font_family=basic_font,
                    ),
                    self.header(logo, self.title(title)),
                    self.main(**kwargs),
                    footer,
                ),
                tags.Text(table_ie9_close),
            ),
        )
        res = html.render()
        if display:
            IPython.core.display.display(IPython.core.display.HTML(res))
        return res
