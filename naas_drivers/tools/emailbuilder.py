from naas_drivers.driver import InDriver
import pandas as pd
import requests
import uuid
import os
import warnings
from IPython.core import display
from htmlBuilder import tags, attributes

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

a.link { color: #B200FD; font-weight: 600; text-decoration: underline; }
a.link:hover { color: #000000 !important; text-decoration: none !important; background-color: #5c1958 !important; }
a.button:hover { color: #ffffff !important; background-color: #5c1958 !important; }

td, th {
    padding: 10px;
}

th {
    text-align: center;
}

table * {
    margin: 18px 0 !important;
    font-size: 18px;
}

.table_border {
  border-collapse: collapse;
  overflow: hidden;
}
.table_border tr:hover {
  background-color: AliceBlue !important;
  color: black;
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

# Link Follow us
GIT_LINK = "https://github.com/jupyter-naas/naas"
YT_LINK = "https://www.youtube.com/channel/UCKKG5hzjXXU_rRdHHWQ8JHQ?sub_confirmation=1"
LK_LINK = "https://www.linkedin.com/showcase/naas-ai"
TW_LINK = "https://twitter.com/JupyterNaas"

# Logo Follow us
GIT_IMG_BLACK = "https://icons.iconarchive.com/icons/icons8/windows-8/512/Programming-Github-icon.png"
YT_IMG_BLACK = "https://icons.iconarchive.com/icons/icons8/windows-8/512/Social-Networks-Youtube-icon.png"
LK_IMG_BLACK = "https://icons.iconarchive.com/icons/icons8/windows-8/512/Social-Networks-Linkedin-icon.png"
TW_IMG_BLACK = "https://icons.iconarchive.com/icons/icons8/windows-8/512/Social-Networks-Twitter-icon.png"


class Align(attributes.HtmlTagAttribute):
    """Specifies alignement position"""


class EmailBuilder(InDriver):
    """EmailBuilder generator lib"""

    deprecated = False

    def __init__(self, deprecated=False):
        self.deprecated = deprecated

    def deprecatedPrint(self, function=None):
        # TODO remove this in june 2021
        if self.deprecated:
            warnings.warn(
                """[Warning], naas_drivers.html or naas_drivers.emailBuilder is deprecated,
                use naas_drivers.emailbuilder instead it will be remove in 1 june 2021"""
            )
        if function:
            # TODO remove this in July 2021
            warnings.warn(
                f"""[Warning], The function emailbuilder.{function}() is deprecated,
                it will be remove in 1 July 2021"""
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

    def __create_network_icon(self, img_src, href, width, padding, margin, bg_color):
        return tags.A(
            attributes.Href(href),
            attributes.Class("link"),
            attributes.Target("_blank"),
            tags.Img(
                attributes.Width("6%"),
                Align("center"),
                attributes.Src(img_src),
                attributes.InlineStyle(
                    padding=padding,
                    margin=margin,
                    border_radius="20%",
                    background_color=bg_color,
                ),
            ),
        )

    def __default_networks(self):
        return [
            {"img_src": GIT_IMG_BLACK, "href": GIT_LINK},
            {"img_src": YT_IMG_BLACK, "href": YT_LINK},
            {"img_src": LK_IMG_BLACK, "href": LK_LINK},
            {"img_src": TW_IMG_BLACK, "href": TW_LINK},
        ]

    def __default_company(self):
        return [
            "Naas: Notebooks-as-a-service",
            "<a target='_blank' href='https://www.naas.ai/'>www.naas.ai</a>",
        ]

    def __default_legal(self):
        return [
            "Powered by CASHSTORY © 2021",
            "CashStory SAS, 5 rue Hermel, 75018 Paris, France",
        ]

    def __text_style(
        self,
        style={},
        color=None,
        font_size=None,
        text_align=None,
        bold=False,
        underline=False,
        italic=False,
    ):
        if color:
            style["color"] = color
        if font_size:
            style["font_size"] = f"{font_size}"
        if text_align:
            style["text_align"] = text_align
        if bold:
            style["font_weight"] = "bold"
        if underline:
            style["text_decoration"] = "underline"
        if italic:
            style["font_style"] = "italic"
        return style

    def __create_table_cell(
        self,
        content,
        header=False,
        align=None,
        border=False,
        header_bg_color="black",
        header_ft_color="white",
        border_color="black",
        size=None,
    ):
        style = {}
        if border:
            style["border"] = f"1px solid {border_color}"
        if size:
            style["width"] = f"{size}"
        if header:
            style["color"] = header_ft_color
            style["background_color"] = header_bg_color
            return tags.Th(content, attributes.InlineStyle(**style))
        else:
            if align is not None:
                style["text_align"] = align
            return tags.Td(content, attributes.InlineStyle(**style))

    def __df_table_header(
        self,
        df,
        border=False,
        header_bg_color="black",
        header_ft_color="white",
        border_color="black",
    ):
        res = []
        for col in df.columns:
            res.append(
                self.__create_table_cell(
                    tags.Text(col), True, None, border, header_bg_color, header_ft_color
                )
            )
        return res

    def title(
        self,
        title,
        heading=None,
        color="#000000",
        font_size="32px",
        text_align="center",
        bold=True,
        underline=False,
        italic=False,
    ):
        self.deprecatedPrint()
        style = {"font_weight": "normal", "line_height": "32px", "margin": "48px 0"}
        style = self.__text_style(
            style, color, font_size, text_align, bold, underline, italic
        )
        return tags.H1(
            attributes.InlineStyle(**style),
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

    def heading(
        self,
        text,
        color="#000000",
        font_size=28,
        text_align="center",
        bold=True,
        underline=False,
        italic=False,
    ):
        self.deprecatedPrint()
        style = {
            "font-weight": "normal",
            "line_height": "32px",
            "margin": "48px 0 24px 0",
            "text_align": text_align,
        }
        style = self.__text_style(
            style, color, font_size, text_align, bold, underline, italic
        )
        return tags.H2(
            attributes.InlineStyle(**style),
            tags.Text(text),
        )

    def subheading(
        self,
        text,
        color="#000000",
        font_size="24px",
        text_align="left",
        bold=False,
        underline=False,
        italic=False,
    ):
        self.deprecatedPrint()
        return self.text(text, color, font_size, text_align, bold, underline, italic)

    def text(
        self,
        text,
        color="#000000",
        font_size="18px",
        text_align=None,
        bold=False,
        underline=False,
        italic=False,
        padding_left="10px",
        padding_right="10px",
    ):
        self.deprecatedPrint()
        style = {"padding_left": padding_left, "padding_right": padding_right}
        style = self.__text_style(
            style, color, font_size, text_align, bold, underline, italic
        )
        return tags.P(
            tags.Text(text),
            attributes.InlineStyle(**style),
        )

    def link(
        self,
        link,
        title="Open",
        color="#B200FD",
        font_size="18px",
        text_align="left",
        bold=False,
        underline=True,
        italic=False,
    ):
        self.deprecatedPrint()
        style = self.__text_style(
            {}, color, font_size, text_align, bold, underline, italic
        )
        return tags.A(
            attributes.Class("link"),
            attributes.Href(link),
            attributes.InlineStyle(**style),
            tags.Text(title),
        )

    def list(self, list_):
        elems = []
        for elem in list_:
            elems.append(
                tags.Li(
                    elem
                    if issubclass(type(elem), tags.HtmlTag)
                    else tags.Text(f"{elem}")
                )
            )
        return tags.Ul(elems)

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
                    attributes.Class("button link"),
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

    def table(
        self,
        data,
        border=False,
        header=False,
        col_align={},
        header_bg_color="black",
        header_ft_color="white",
        border_color="black",
        col_size={},
    ):
        self.deprecatedPrint()
        elems = []
        table_arr = None
        row_link = False
        row_link_index = -1
        index = 0
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
            is_header = index == 0 and header is True
            if isinstance(row, list):
                for i in range(len(row)):
                    align = col_align[i] if i in col_align else None
                    size = col_size[i] if i in col_size else None
                    cell = row[i]

                    if isinstance(data, pd.DataFrame):
                        col = list(data.columns)[i]
                        if is_header:
                            elems.append(
                                tags.Tr(
                                    self.__df_table_header(
                                        data,
                                        border,
                                        header_bg_color,
                                        header_ft_color,
                                        border_color,
                                    )
                                )
                            )
                            is_header = False
                        if row_link:
                            if col != "row_link":
                                link = row[row_link_index]
                                res.append(
                                    self.__create_table_cell(
                                        tags.A(
                                            attributes.Class("link"),
                                            attributes.Href(link),
                                            self.__convert(cell, col),
                                        ),
                                        header=is_header,
                                        align=align,
                                        border=border,
                                        header_bg_color=header_bg_color,
                                        header_ft_color=header_ft_color,
                                        border_color=border_color,
                                    )
                                )
                        else:
                            res.append(
                                self.__create_table_cell(
                                    self.__convert(cell, col),
                                    is_header,
                                    align,
                                    border,
                                    header_bg_color,
                                    header_ft_color,
                                    border_color,
                                    size,
                                )
                            )
                    else:
                        res.append(
                            self.__create_table_cell(
                                (tags.Text(cell) if isinstance(cell, str) else cell),
                                is_header,
                                align,
                                border,
                                header_bg_color,
                                header_ft_color,
                                border_color,
                                size,
                            )
                        )
            else:
                align = col_align[0] if 0 in col_align else None
                size = col_size[0] if 0 in col_size else None
                res.append(
                    self.__create_table_cell(
                        (tags.Text(row) if isinstance(row, str) else row),
                        is_header,
                        align,
                        border,
                        header_bg_color,
                        header_ft_color,
                        border_color,
                        size,
                    )
                )
            elems.append(tags.Tr(res))
            index += 1
        tab = tags.Table(
            attributes.InlineStyle(width="100%", border_collapse="collapse"),
            attributes.Class("table_border") if border else None,
            elems,
        )
        return tags.P(tab)

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
            return tags.A(
                attributes.Class("link"), attributes.Href(link), tags.Img(elems_img)
            )
        else:
            return tags.Img(elems_img)

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

    def footer_company(
        self,
        networks=None,
        company=None,
        legal=None,
        logo_width="6%",
        logo_padding="5px",
        logo_margin="0px 15px",
        logo_bg_color="white",
        naas=False,
    ):
        self.deprecatedPrint()
        net = []
        com = []
        leg = []
        if naas:
            networks = self.__default_networks()
            company = self.__default_company()
            # legal = self.__default_legal()
        if networks:
            for elem in networks:
                if elem.keys() >= frozenset({"img_src", "href"}):
                    net.append(
                        self.__create_network_icon(
                            **elem,
                            width=logo_width,
                            padding=logo_padding,
                            margin=logo_margin,
                            bg_color=logo_bg_color,
                        )
                    )
            net = tags.P(net, attributes.InlineStyle(text_align="center"))
        if company:
            for elem in company:
                com.append(tags.B(tags.Text(elem)))
                com.append(tags.Br())
            com = tags.P(
                attributes.InlineStyle(line_height=1, text_align="center"),
                tags.Span(com, attributes.InlineStyle(font_size="12px")),
            )
        if legal:
            for elem in legal:
                leg.append(tags.Text(elem))
                leg.append(tags.Br())
            leg = tags.P(
                attributes.InlineStyle(line_height=1, text_align="center"),
                tags.Span(leg, attributes.InlineStyle(font_size="11px")),
            )
        return tags.Footer(tags.Hr(), net, com, leg)

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
            display.display(display.HTML(shadow))
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
            display.display(display.HTML(iframe))
        elif mode == "embed":
            display.display(display.HTML(content))

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
            url=f"{os.environ.get('SCREENSHOT_API', 'http://naas-screenshot:9000')}/api/render",  # Sensitive
            json=json,
        )
        req.raise_for_status()
        open(filename, "wb").write(req.content)
        print(f"Saved as {filename}")

    def export(self, html, filenames, css=None):
        """create html export and add css to it"""
        self.deprecatedPrint()
        if isinstance(filenames, list):
            for filename in filenames:
                self.__export(html, filename, css)
        else:
            self.__export(html, filenames, css)

    def generate(self, title=None, logo=None, display="embed", footer=None, **kwargs):
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
                tags.Title(tags.Text(title)) if title else None,
            ),
            tags.Body(
                attributes.InlineStyle(margin="0 !important", padding="0 !important"),
                tags.Div(
                    attributes.InlineStyle(
                        display="none", max_height="0", overflow="hidden"
                    ),
                    tags.Text(title) if title else None,
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
                    {"name": "aria-label", "value": title} if title else None,
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
                    self.title(title) if title else None,
                    self.main(**kwargs),
                    footer,
                ),
                tags.Text(table_ie9_close),
            ),
        )
        res = gen_html.render()
        self.__display(res, display)
        return res
