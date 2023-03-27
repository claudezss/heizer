# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
# import os
# import sys

# sys.path.insert(0, os.path.abspath("../.."))

project = "heizer"
copyright = "2023, Yan Zhang"
author = "Yan Zhang"
release = "main"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx_multiversion",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "IPython.sphinxext.ipython_directive",
    "IPython.sphinxext.ipython_console_highlighting",
]

templates_path = [
    "_templates",
]


autodoc_typehints = "description"


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_rtd_theme"
html_static_path = ["_static"]

html_theme_options = {
    "analytics_id": "G-5RY57HDL43",
    "analytics_anonymize_ip": False,
    "display_version": False,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "vcs_pageview_mode": "",
}


smv_tag_whitelist = r"^.*$"
smv_branch_whitelist = r"main"
smv_remote_whitelist = r"^(origin|upstream)$"
smv_released_pattern = r"^tags/.*$"
