import requests
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class Parameter:
    name: str
    type: str
    default: str
    details: str

@dataclass
class Function:
    name: str
    signature: str
    description: str
    parameters: List[Parameter] = field(default_factory=list)
    returns: Optional[Parameter] = None

def parse_function(section):
    name = section.find('h3').text.strip()
    signature = section.find('pre').text.strip()
    description = section.find('p', {'class': 'anchored'}).find_next_sibling('p').text.strip()
    
    function = Function(name=name, signature=signature, description=description)
    
    table = section.find('table')
    if table:
        rows = table.find_all('tr')[1:]  # Skip header row
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 4:
                param = Parameter(
                    name=cols[0].text.strip(),
                    type=cols[1].text.strip(),
                    default=cols[2].text.strip(),
                    details=cols[3].text.strip()
                )
                if param.name == "Returns":
                    function.returns = param
                else:
                    function.parameters.append(param)
    
    return function

def scrape_fasthtml_api(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    functions = []
    for section in soup.find_all('section', class_='level3'):
        function = parse_function(section)
        functions.append(function)
    
    return functions

# Use the provided HTML content
html_content = '''

<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en"><head>

<meta charset="utf-8">
<meta name="generator" content="quarto-1.5.56">

<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">


<title>Serving FastHTML apps – fasthtml</title>
<style>
code{white-space: pre-wrap;}
span.smallcaps{font-variant: small-caps;}
div.columns{display: flex; gap: min(4vw, 1.5em);}
div.column{flex: auto; overflow-x: auto;}
div.hanging-indent{margin-left: 1.5em; text-indent: -1.5em;}
ul.task-list{list-style: none;}
ul.task-list li input[type="checkbox"] {
  width: 0.8em;
  margin: 0 0.8em 0.2em -1em; /* quarto-specific, see https://github.com/quarto-dev/quarto-cli/issues/4556 */ 
  vertical-align: middle;
}
/* CSS for syntax highlighting */
pre > code.sourceCode { white-space: pre; position: relative; }
pre > code.sourceCode > span { line-height: 1.25; }
pre > code.sourceCode > span:empty { height: 1.2em; }
.sourceCode { overflow: visible; }
code.sourceCode > span { color: inherit; text-decoration: inherit; }
div.sourceCode { margin: 1em 0; }
pre.sourceCode { margin: 0; }
@media screen {
div.sourceCode { overflow: auto; }
}
@media print {
pre > code.sourceCode { white-space: pre-wrap; }
pre > code.sourceCode > span { display: inline-block; text-indent: -5em; padding-left: 5em; }
}
pre.numberSource code
  { counter-reset: source-line 0; }
pre.numberSource code > span
  { position: relative; left: -4em; counter-increment: source-line; }
pre.numberSource code > span > a:first-child::before
  { content: counter(source-line);
    position: relative; left: -1em; text-align: right; vertical-align: baseline;
    border: none; display: inline-block;
    -webkit-touch-callout: none; -webkit-user-select: none;
    -khtml-user-select: none; -moz-user-select: none;
    -ms-user-select: none; user-select: none;
    padding: 0 4px; width: 4em;
  }
pre.numberSource { margin-left: 3em;  padding-left: 4px; }
div.sourceCode
  {   }
@media screen {
pre > code.sourceCode > span > a:first-child::before { text-decoration: underline; }
}
</style>


<script src="../site_libs/quarto-nav/quarto-nav.js"></script>
<script src="../site_libs/quarto-nav/headroom.min.js"></script>
<script src="../site_libs/clipboard/clipboard.min.js"></script>
<script src="../site_libs/quarto-search/autocomplete.umd.js"></script>
<script src="../site_libs/quarto-search/fuse.min.js"></script>
<script src="../site_libs/quarto-search/quarto-search.js"></script>
<meta name="quarto:offset" content="../">
<script src="../site_libs/quarto-html/quarto.js"></script>
<script src="../site_libs/quarto-html/popper.min.js"></script>
<script src="../site_libs/quarto-html/tippy.umd.min.js"></script>
<script src="../site_libs/quarto-html/anchor.min.js"></script>
<link href="../site_libs/quarto-html/tippy.css" rel="stylesheet">
<link href="../site_libs/quarto-html/quarto-syntax-highlighting.css" rel="stylesheet" id="quarto-text-highlighting-styles">
<script src="../site_libs/bootstrap/bootstrap.min.js"></script>
<link href="../site_libs/bootstrap/bootstrap-icons.css" rel="stylesheet">
<link href="../site_libs/bootstrap/bootstrap.min.css" rel="stylesheet" id="quarto-bootstrap" data-mode="light">
<script id="quarto-search-options" type="application/json">{
  "location": "navbar",
  "copy-button": false,
  "collapse-after": 3,
  "panel-placement": "end",
  "type": "overlay",
  "limit": 50,
  "keyboard-shortcut": [
    "f",
    "/",
    "s"
  ],
  "show-item-context": false,
  "language": {
    "search-no-results-text": "No results",
    "search-matching-documents-text": "matching documents",
    "search-copy-link-title": "Copy link to search",
    "search-hide-matches-text": "Hide additional matches",
    "search-more-match-text": "more match in this document",
    "search-more-matches-text": "more matches in this document",
    "search-clear-button-title": "Clear",
    "search-text-placeholder": "",
    "search-detached-cancel-button-title": "Cancel",
    "search-submit-button-title": "Submit",
    "search-label": "Search"
  }
}</script>


<link rel="stylesheet" href="../styles.css">
<meta property="og:title" content="Serving FastHTML apps – fasthtml">
<meta property="og:description" content="The fastest way to create an HTML app">
<meta property="og:site_name" content="fasthtml">
<meta name="twitter:title" content="Serving FastHTML apps – fasthtml">
<meta name="twitter:description" content="The fastest way to create an HTML app">
<meta name="twitter:card" content="summary">
</head>

<body class="nav-sidebar floating nav-fixed">

<div id="quarto-search-results"></div>
  <header id="quarto-header" class="headroom fixed-top">
    <nav class="navbar navbar-expand-lg " data-bs-theme="dark">
      <div class="navbar-container container-fluid">
      <div class="navbar-brand-container mx-auto">
    <a href="../index.html" class="navbar-brand navbar-brand-logo">
    <img src="../logo.svg" alt="" class="navbar-logo">
    </a>
  </div>
            <div id="quarto-search" class="" title="Search"></div>
          <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarCollapse" aria-controls="navbarCollapse" role="menu" aria-expanded="false" aria-label="Toggle navigation" onclick="if (window.quartoToggleHeadroom) { window.quartoToggleHeadroom(); }">
  <span class="navbar-toggler-icon"></span>
</button>
          <div class="collapse navbar-collapse" id="navbarCollapse">
            <ul class="navbar-nav navbar-nav-scroll me-auto">
  <li class="nav-item">
    <a class="nav-link" href="https://fastht.ml"> 
<span class="menu-text">Home</span></a>
  </li>  
  <li class="nav-item">
    <a class="nav-link" href="https://about.fastht.ml"> 
<span class="menu-text">Learn</span></a>
  </li>  
</ul>
            <ul class="navbar-nav navbar-nav-scroll ms-auto">
  <li class="nav-item compact">
    <a class="nav-link" href="https://github.com/answerdotai/fasthtml"> <i class="bi bi-github" role="img">
</i> 
<span class="menu-text"></span></a>
  </li>  
  <li class="nav-item compact">
    <a class="nav-link" href="https://x.com/answerdotai"> <i class="bi bi-twitter" role="img" aria-label="Fast.ai Twitter">
</i> 
<span class="menu-text"></span></a>
  </li>  
</ul>
          </div> <!-- /navcollapse -->
            <div class="quarto-navbar-tools">
</div>
      </div> <!-- /container-fluid -->
    </nav>
  <nav class="quarto-secondary-nav">
    <div class="container-fluid d-flex">
      <button type="button" class="quarto-btn-toggle btn" data-bs-toggle="collapse" role="button" data-bs-target=".quarto-sidebar-collapse-item" aria-controls="quarto-sidebar" aria-expanded="false" aria-label="Toggle sidebar navigation" onclick="if (window.quartoToggleHeadroom) { window.quartoToggleHeadroom(); }">
        <i class="bi bi-layout-text-sidebar-reverse"></i>
      </button>
        <nav class="quarto-page-breadcrumbs" aria-label="breadcrumb"><ol class="breadcrumb"><li class="breadcrumb-item"><a href="../api/core.html">Source</a></li><li class="breadcrumb-item"><a href="../api/fastapp.html">Serving FastHTML apps</a></li></ol></nav>
        <a class="flex-grow-1" role="navigation" data-bs-toggle="collapse" data-bs-target=".quarto-sidebar-collapse-item" aria-controls="quarto-sidebar" aria-expanded="false" aria-label="Toggle sidebar navigation" onclick="if (window.quartoToggleHeadroom) { window.quartoToggleHeadroom(); }">      
        </a>
    </div>
  </nav>
</header>
<!-- content -->
<div id="quarto-content" class="quarto-container page-columns page-rows-contents page-layout-article page-navbar">
<!-- sidebar -->
  <nav id="quarto-sidebar" class="sidebar collapse collapse-horizontal quarto-sidebar-collapse-item sidebar-navigation floating overflow-auto">
    <div class="sidebar-menu-container"> 
    <ul class="list-unstyled mt-1">
        <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../index.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Get Started</span></a>
  </div>
</li>
        <li class="sidebar-item sidebar-item-section">
      <div class="sidebar-item-container"> 
            <a href="../tutorials/index.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Tutorials</span></a>
          <a class="sidebar-item-toggle text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-1" role="navigation" aria-expanded="true" aria-label="Toggle section">
            <i class="bi bi-chevron-right ms-2"></i>
          </a> 
      </div>
      <ul id="quarto-sidebar-section-1" class="collapse list-unstyled sidebar-section depth1 show">  
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../tutorials/by_example.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">FastHTML By Example</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../tutorials/quickstart_for_web_devs.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Web Devs Quickstart</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../tutorials/e2e.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">JS App Walkthrough</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../tutorials/tutorial_for_web_devs.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">BYO Blog</span></a>
  </div>
</li>
      </ul>
  </li>
        <li class="sidebar-item sidebar-item-section">
      <div class="sidebar-item-container"> 
            <a class="sidebar-item-text sidebar-link text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-2" role="navigation" aria-expanded="true">
 <span class="menu-text">Explanations</span></a>
          <a class="sidebar-item-toggle text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-2" role="navigation" aria-expanded="true" aria-label="Toggle section">
            <i class="bi bi-chevron-right ms-2"></i>
          </a> 
      </div>
      <ul id="quarto-sidebar-section-2" class="collapse list-unstyled sidebar-section depth1 show">  
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../explains/explaining_xt_components.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text"><strong>ft</strong> Components</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../explains/faq.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">FAQ</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../explains/routes.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Routes</span></a>
  </div>
</li>
      </ul>
  </li>
        <li class="sidebar-item sidebar-item-section">
      <div class="sidebar-item-container"> 
            <a class="sidebar-item-text sidebar-link text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-3" role="navigation" aria-expanded="true">
 <span class="menu-text">Reference</span></a>
          <a class="sidebar-item-toggle text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-3" role="navigation" aria-expanded="true" aria-label="Toggle section">
            <i class="bi bi-chevron-right ms-2"></i>
          </a> 
      </div>
      <ul id="quarto-sidebar-section-3" class="collapse list-unstyled sidebar-section depth1 show">  
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../ref/defining_xt_component.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Custom Components</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../ref/live_reload.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Live Reloading</span></a>
  </div>
</li>
      </ul>
  </li>
        <li class="sidebar-item sidebar-item-section">
      <div class="sidebar-item-container"> 
            <a class="sidebar-item-text sidebar-link text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-4" role="navigation" aria-expanded="true">
 <span class="menu-text">Source</span></a>
          <a class="sidebar-item-toggle text-start" data-bs-toggle="collapse" data-bs-target="#quarto-sidebar-section-4" role="navigation" aria-expanded="true" aria-label="Toggle section">
            <i class="bi bi-chevron-right ms-2"></i>
          </a> 
      </div>
      <ul id="quarto-sidebar-section-4" class="collapse list-unstyled sidebar-section depth1 show">  
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../api/core.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Core</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../api/components.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Components</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../api/xtend.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Component extensions</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../api/cli.html" class="sidebar-item-text sidebar-link">
 <span class="menu-text">Command Line Tools</span></a>
  </div>
</li>
          <li class="sidebar-item">
  <div class="sidebar-item-container"> 
  <a href="../api/fastapp.html" class="sidebar-item-text sidebar-link active">
 <span class="menu-text">Serving FastHTML apps</span></a>
  </div>
</li>
      </ul>
  </li>
    </ul>
    </div>
</nav>
<div id="quarto-sidebar-glass" class="quarto-sidebar-collapse-item" data-bs-toggle="collapse" data-bs-target=".quarto-sidebar-collapse-item"></div>
<!-- margin-sidebar -->
    <div id="quarto-margin-sidebar" class="sidebar margin-sidebar">
        <nav id="TOC" role="doc-toc" class="toc-active">
    <h2 id="toc-title">On this page</h2>
   
  <ul>
  <li><a href="#fast_app" id="toc-fast_app" class="nav-link active" data-scroll-target="#fast_app">fast_app</a></li>
  <li><a href="#serve" id="toc-serve" class="nav-link" data-scroll-target="#serve">serve</a></li>
  </ul>
<div class="toc-actions"><ul><li><a href="https://github.com/AnswerDotAI/fasthtml/issues/new" class="toc-action"><i class="bi bi-github"></i>Report an issue</a></li></ul></div></nav>
    </div>
<!-- main -->
<main class="content" id="quarto-document-content">

<header id="title-block-header" class="quarto-title-block default"><nav class="quarto-page-breadcrumbs quarto-title-breadcrumbs d-none d-lg-block" aria-label="breadcrumb"><ol class="breadcrumb"><li class="breadcrumb-item"><a href="../api/core.html">Source</a></li><li class="breadcrumb-item"><a href="../api/fastapp.html">Serving FastHTML apps</a></li></ol></nav>
<div class="quarto-title">
<h1 class="title">Serving FastHTML apps</h1>
</div>



<div class="quarto-title-meta">

    
  
    
  </div>
  


</header>


<!-- WARNING: THIS FILE WAS AUTOGENERATED! DO NOT EDIT! -->
<p>The preferred method of serving FastHTML apps is via the functions <a href="https://AnswerDotAI.github.io/fasthtml/api/fastapp.html#fast_app"><code>fast_app</code></a> and <a href="https://AnswerDotAI.github.io/fasthtml/api/fastapp.html#serve"><code>serve</code></a>. Usage can be summarized as:</p>
<div class="sourceCode" id="cb1"><pre class="sourceCode python code-with-copy"><code class="sourceCode python"><span id="cb1-1"><a href="#cb1-1" aria-hidden="true" tabindex="-1"></a><span class="im">from</span> fasthtml.common <span class="im">import</span> <span class="op">*</span></span>
<span id="cb1-2"><a href="#cb1-2" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb1-3"><a href="#cb1-3" aria-hidden="true" tabindex="-1"></a>app, rt <span class="op">=</span> fast_app()</span>
<span id="cb1-4"><a href="#cb1-4" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb1-5"><a href="#cb1-5" aria-hidden="true" tabindex="-1"></a><span class="at">@rt</span>(<span class="st">'/'</span>)</span>
<span id="cb1-6"><a href="#cb1-6" aria-hidden="true" tabindex="-1"></a><span class="kw">def</span> get():</span>
<span id="cb1-7"><a href="#cb1-7" aria-hidden="true" tabindex="-1"></a>    <span class="cf">return</span> Titled(<span class="st">"A simple demo of fast_app() and serve()"</span>)</span>
<span id="cb1-8"><a href="#cb1-8" aria-hidden="true" tabindex="-1"></a></span>
<span id="cb1-9"><a href="#cb1-9" aria-hidden="true" tabindex="-1"></a>serve()</span></code><button title="Copy to Clipboard" class="code-copy-button"><i class="bi"></i></button></pre></div>
<hr>
<p><a href="https://github.com/AnswerDotAI/fasthtml/blob/main/fasthtml/fastapp.py#L34" target="_blank" style="float:right; font-size:smaller">source</a></p>
<section id="fast_app" class="level3">
<h3 class="anchored" data-anchor-id="fast_app">fast_app</h3>
<blockquote class="blockquote">
<pre><code> fast_app (db_file:Optional[str]=None, render:Optional[&lt;built-
           infunctioncallable&gt;]=None, hdrs:Optional[tuple]=None,
           ftrs:Optional[tuple]=None, tbls:Optional[dict]=None,
           before:Union[tuple,NoneType,fasthtml.core.Beforeware]=None,
           middleware:Optional[tuple]=None, live:bool=False,
           debug:bool=False, routes:Optional[tuple]=None,
           exception_handlers:Optional[dict]=None,
           on_startup:Optional[&lt;built-infunctioncallable&gt;]=None,
           on_shutdown:Optional[&lt;built-infunctioncallable&gt;]=None,
           lifespan:Optional[&lt;built-infunctioncallable&gt;]=None,
           default_hdrs=True, pico:Optional[bool]=None,
           surreal:Optional[bool]=True, htmx:Optional[bool]=True,
           ws_hdr:bool=False, secret_key:Optional[str]=None,
           key_fname:str='.sesskey', session_cookie:str='session_',
           max_age:int=31536000, sess_path:str='/', same_site:str='lax',
           sess_https_only:bool=False, sess_domain:Optional[str]=None,
           htmlkw:Optional[dict]=None, bodykw:Optional[dict]=None,
           reload_attempts:Optional[int]=1,
           reload_interval:Optional[int]=1000, **kwargs)</code></pre>
</blockquote>
<p><em>Create a FastHTML or FastHTMLWithLiveReload app.</em></p>
<table class="caption-top table">
<colgroup>
<col style="width: 6%">
<col style="width: 25%">
<col style="width: 34%">
<col style="width: 34%">
</colgroup>
<thead>
<tr class="header">
<th></th>
<th><strong>Type</strong></th>
<th><strong>Default</strong></th>
<th><strong>Details</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>db_file</td>
<td>Optional[str]</td>
<td>None</td>
<td>Database file name, if needed</td>
</tr>
<tr class="even">
<td>render</td>
<td>Optional[callable]</td>
<td>None</td>
<td>Function used to render default database class</td>
</tr>
<tr class="odd">
<td>hdrs</td>
<td>Optional[tuple]</td>
<td>None</td>
<td>Additional FT elements to add to </td>
</tr>
<tr class="even">
<td>ftrs</td>
<td>Optional[tuple]</td>
<td>None</td>
<td>Additional FT elements to add to end of </td>
</tr>
<tr class="odd">
<td>tbls</td>
<td>Optional[dict]</td>
<td>None</td>
<td>Experimental mapping from DB table names to dict table definitions</td>
</tr>
<tr class="even">
<td>before</td>
<td>Optional[tuple] | Beforeware</td>
<td>None</td>
<td>Functions to call prior to calling handler</td>
</tr>
<tr class="odd">
<td>middleware</td>
<td>Optional[tuple]</td>
<td>None</td>
<td>Standard Starlette middleware</td>
</tr>
<tr class="even">
<td>live</td>
<td>bool</td>
<td>False</td>
<td>Enable live reloading</td>
</tr>
<tr class="odd">
<td>debug</td>
<td>bool</td>
<td>False</td>
<td>Passed to Starlette, indicating if debug tracebacks should be returned on errors</td>
</tr>
<tr class="even">
<td>routes</td>
<td>Optional[tuple]</td>
<td>None</td>
<td>Passed to Starlette</td>
</tr>
<tr class="odd">
<td>exception_handlers</td>
<td>Optional[dict]</td>
<td>None</td>
<td>Passed to Starlette</td>
</tr>
<tr class="even">
<td>on_startup</td>
<td>Optional[callable]</td>
<td>None</td>
<td>Passed to Starlette</td>
</tr>
<tr class="odd">
<td>on_shutdown</td>
<td>Optional[callable]</td>
<td>None</td>
<td>Passed to Starlette</td>
</tr>
<tr class="even">
<td>lifespan</td>
<td>Optional[callable]</td>
<td>None</td>
<td>Passed to Starlette</td>
</tr>
<tr class="odd">
<td>default_hdrs</td>
<td>bool</td>
<td>True</td>
<td>Include default FastHTML headers such as HTMX script?</td>
</tr>
<tr class="even">
<td>pico</td>
<td>Optional[bool]</td>
<td>None</td>
<td>Include PicoCSS header?</td>
</tr>
<tr class="odd">
<td>surreal</td>
<td>Optional[bool]</td>
<td>True</td>
<td>Include surreal.js/scope headers?</td>
</tr>
<tr class="even">
<td>htmx</td>
<td>Optional[bool]</td>
<td>True</td>
<td>Include HTMX header?</td>
</tr>
<tr class="odd">
<td>ws_hdr</td>
<td>bool</td>
<td>False</td>
<td>Include HTMX websocket extension header?</td>
</tr>
<tr class="even">
<td>secret_key</td>
<td>Optional[str]</td>
<td>None</td>
<td>Signing key for sessions</td>
</tr>
<tr class="odd">
<td>key_fname</td>
<td>str</td>
<td>.sesskey</td>
<td>Session cookie signing key file name</td>
</tr>
<tr class="even">
<td>session_cookie</td>
<td>str</td>
<td>session_</td>
<td>Session cookie name</td>
</tr>
<tr class="odd">
<td>max_age</td>
<td>int</td>
<td>31536000</td>
<td>Session cookie expiry time</td>
</tr>
<tr class="even">
<td>sess_path</td>
<td>str</td>
<td>/</td>
<td>Session cookie path</td>
</tr>
<tr class="odd">
<td>same_site</td>
<td>str</td>
<td>lax</td>
<td>Session cookie same site policy</td>
</tr>
<tr class="even">
<td>sess_https_only</td>
<td>bool</td>
<td>False</td>
<td>Session cookie HTTPS only?</td>
</tr>
<tr class="odd">
<td>sess_domain</td>
<td>Optional[str]</td>
<td>None</td>
<td>Session cookie domain</td>
</tr>
<tr class="even">
<td>htmlkw</td>
<td>Optional[dict]</td>
<td>None</td>
<td>Attrs to add to the HTML tag</td>
</tr>
<tr class="odd">
<td>bodykw</td>
<td>Optional[dict]</td>
<td>None</td>
<td>Attrs to add to the Body tag</td>
</tr>
<tr class="even">
<td>reload_attempts</td>
<td>Optional[int]</td>
<td>1</td>
<td>Number of reload attempts when live reloading</td>
</tr>
<tr class="odd">
<td>reload_interval</td>
<td>Optional[int]</td>
<td>1000</td>
<td>Time between reload attempts in ms</td>
</tr>
<tr class="even">
<td>kwargs</td>
<td></td>
<td></td>
<td></td>
</tr>
<tr class="odd">
<td><strong>Returns</strong></td>
<td><strong>Any</strong></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>
<hr>
<p><a href="https://github.com/AnswerDotAI/fasthtml/blob/main/fasthtml/fastapp.py#L93" target="_blank" style="float:right; font-size:smaller">source</a></p>
</section>
<section id="serve" class="level3">
<h3 class="anchored" data-anchor-id="serve">serve</h3>
<blockquote class="blockquote">
<pre><code> serve (appname=None, app='app', host='0.0.0.0', port=None, reload=True)</code></pre>
</blockquote>
<p><em>Run the app in an async server, with live reload set as the default.</em></p>
<table class="caption-top table">
<colgroup>
<col style="width: 6%">
<col style="width: 25%">
<col style="width: 34%">
<col style="width: 34%">
</colgroup>
<thead>
<tr class="header">
<th></th>
<th><strong>Type</strong></th>
<th><strong>Default</strong></th>
<th><strong>Details</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>appname</td>
<td>NoneType</td>
<td>None</td>
<td>Name of the module</td>
</tr>
<tr class="even">
<td>app</td>
<td>str</td>
<td>app</td>
<td>App instance to be served</td>
</tr>
<tr class="odd">
<td>host</td>
<td>str</td>
<td>0.0.0.0</td>
<td>If host is 0.0.0.0 will convert to localhost</td>
</tr>
<tr class="even">
<td>port</td>
<td>NoneType</td>
<td>None</td>
<td>If port is None it will default to 5001 or the PORT environment variable</td>
</tr>
<tr class="odd">
<td>reload</td>
<td>bool</td>
<td>True</td>
<td>Default is to reload the app upon code changes</td>
</tr>
</tbody>
</table>


</section>

</main> <!-- /main -->
<script id="quarto-html-after-body" type="application/javascript">
window.document.addEventListener("DOMContentLoaded", function (event) {
  const toggleBodyColorMode = (bsSheetEl) => {
    const mode = bsSheetEl.getAttribute("data-mode");
    const bodyEl = window.document.querySelector("body");
    if (mode === "dark") {
      bodyEl.classList.add("quarto-dark");
      bodyEl.classList.remove("quarto-light");
    } else {
      bodyEl.classList.add("quarto-light");
      bodyEl.classList.remove("quarto-dark");
    }
  }
  const toggleBodyColorPrimary = () => {
    const bsSheetEl = window.document.querySelector("link#quarto-bootstrap");
    if (bsSheetEl) {
      toggleBodyColorMode(bsSheetEl);
    }
  }
  toggleBodyColorPrimary();  
  const icon = "";
  const anchorJS = new window.AnchorJS();
  anchorJS.options = {
    placement: 'right',
    icon: icon
  };
  anchorJS.add('.anchored');
  const isCodeAnnotation = (el) => {
    for (const clz of el.classList) {
      if (clz.startsWith('code-annotation-')) {                     
        return true;
      }
    }
    return false;
  }
  const onCopySuccess = function(e) {
    // button target
    const button = e.trigger;
    // don't keep focus
    button.blur();
    // flash "checked"
    button.classList.add('code-copy-button-checked');
    var currentTitle = button.getAttribute("title");
    button.setAttribute("title", "Copied!");
    let tooltip;
    if (window.bootstrap) {
      button.setAttribute("data-bs-toggle", "tooltip");
      button.setAttribute("data-bs-placement", "left");
      button.setAttribute("data-bs-title", "Copied!");
      tooltip = new bootstrap.Tooltip(button, 
        { trigger: "manual", 
          customClass: "code-copy-button-tooltip",
          offset: [0, -8]});
      tooltip.show();    
    }
    setTimeout(function() {
      if (tooltip) {
        tooltip.hide();
        button.removeAttribute("data-bs-title");
        button.removeAttribute("data-bs-toggle");
        button.removeAttribute("data-bs-placement");
      }
      button.setAttribute("title", currentTitle);
      button.classList.remove('code-copy-button-checked');
    }, 1000);
    // clear code selection
    e.clearSelection();
  }
  const getTextToCopy = function(trigger) {
      const codeEl = trigger.previousElementSibling.cloneNode(true);
      for (const childEl of codeEl.children) {
        if (isCodeAnnotation(childEl)) {
          childEl.remove();
        }
      }
      return codeEl.innerText;
  }
  const clipboard = new window.ClipboardJS('.code-copy-button:not([data-in-quarto-modal])', {
    text: getTextToCopy
  });
  clipboard.on('success', onCopySuccess);
  if (window.document.getElementById('quarto-embedded-source-code-modal')) {
    // For code content inside modals, clipBoardJS needs to be initialized with a container option
    // TODO: Check when it could be a function (https://github.com/zenorocha/clipboard.js/issues/860)
    const clipboardModal = new window.ClipboardJS('.code-copy-button[data-in-quarto-modal]', {
      text: getTextToCopy,
      container: window.document.getElementById('quarto-embedded-source-code-modal')
    });
    clipboardModal.on('success', onCopySuccess);
  }
    var localhostRegex = new RegExp(/^(?:http|https):\/\/localhost\:?[0-9]*\//);
    var mailtoRegex = new RegExp(/^mailto:/);
      var filterRegex = new RegExp("https:\/\/AnswerDotAI\.github\.io\/fasthtml");
    var isInternal = (href) => {
        return filterRegex.test(href) || localhostRegex.test(href) || mailtoRegex.test(href);
    }
    // Inspect non-navigation links and adorn them if external
 	var links = window.document.querySelectorAll('a[href]:not(.nav-link):not(.navbar-brand):not(.toc-action):not(.sidebar-link):not(.sidebar-item-toggle):not(.pagination-link):not(.no-external):not([aria-hidden]):not(.dropdown-item):not(.quarto-navigation-tool):not(.about-link)');
    for (var i=0; i<links.length; i++) {
      const link = links[i];
      if (!isInternal(link.href)) {
        // undo the damage that might have been done by quarto-nav.js in the case of
        // links that we want to consider external
        if (link.dataset.originalHref !== undefined) {
          link.href = link.dataset.originalHref;
        }
      }
    }
  function tippyHover(el, contentFn, onTriggerFn, onUntriggerFn) {
    const config = {
      allowHTML: true,
      maxWidth: 500,
      delay: 100,
      arrow: false,
      appendTo: function(el) {
          return el.parentElement;
      },
      interactive: true,
      interactiveBorder: 10,
      theme: 'quarto',
      placement: 'bottom-start',
    };
    if (contentFn) {
      config.content = contentFn;
    }
    if (onTriggerFn) {
      config.onTrigger = onTriggerFn;
    }
    if (onUntriggerFn) {
      config.onUntrigger = onUntriggerFn;
    }
    window.tippy(el, config); 
  }
  const noterefs = window.document.querySelectorAll('a[role="doc-noteref"]');
  for (var i=0; i<noterefs.length; i++) {
    const ref = noterefs[i];
    tippyHover(ref, function() {
      // use id or data attribute instead here
      let href = ref.getAttribute('data-footnote-href') || ref.getAttribute('href');
      try { href = new URL(href).hash; } catch {}
      const id = href.replace(/^#\/?/, "");
      const note = window.document.getElementById(id);
      if (note) {
        return note.innerHTML;
      } else {
        return "";
      }
    });
  }
  const xrefs = window.document.querySelectorAll('a.quarto-xref');
  const processXRef = (id, note) => {
    // Strip column container classes
    const stripColumnClz = (el) => {
      el.classList.remove("page-full", "page-columns");
      if (el.children) {
        for (const child of el.children) {
          stripColumnClz(child);
        }
      }
    }
    stripColumnClz(note)
    if (id === null || id.startsWith('sec-')) {
      // Special case sections, only their first couple elements
      const container = document.createElement("div");
      if (note.children && note.children.length > 2) {
        container.appendChild(note.children[0].cloneNode(true));
        for (let i = 1; i < note.children.length; i++) {
          const child = note.children[i];
          if (child.tagName === "P" && child.innerText === "") {
            continue;
          } else {
            container.appendChild(child.cloneNode(true));
            break;
          }
        }
        if (window.Quarto?.typesetMath) {
          window.Quarto.typesetMath(container);
        }
        return container.innerHTML
      } else {
        if (window.Quarto?.typesetMath) {
          window.Quarto.typesetMath(note);
        }
        return note.innerHTML;
      }
    } else {
      // Remove any anchor links if they are present
      const anchorLink = note.querySelector('a.anchorjs-link');
      if (anchorLink) {
        anchorLink.remove();
      }
      if (window.Quarto?.typesetMath) {
        window.Quarto.typesetMath(note);
      }
      // TODO in 1.5, we should make sure this works without a callout special case
      if (note.classList.contains("callout")) {
        return note.outerHTML;
      } else {
        return note.innerHTML;
      }
    }
  }
  for (var i=0; i<xrefs.length; i++) {
    const xref = xrefs[i];
    tippyHover(xref, undefined, function(instance) {
      instance.disable();
      let url = xref.getAttribute('href');
      let hash = undefined; 
      if (url.startsWith('#')) {
        hash = url;
      } else {
        try { hash = new URL(url).hash; } catch {}
      }
      if (hash) {
        const id = hash.replace(/^#\/?/, "");
        const note = window.document.getElementById(id);
        if (note !== null) {
          try {
            const html = processXRef(id, note.cloneNode(true));
            instance.setContent(html);
          } finally {
            instance.enable();
            instance.show();
          }
        } else {
          // See if we can fetch this
          fetch(url.split('#')[0])
          .then(res => res.text())
          .then(html => {
            const parser = new DOMParser();
            const htmlDoc = parser.parseFromString(html, "text/html");
            const note = htmlDoc.getElementById(id);
            if (note !== null) {
              const html = processXRef(id, note);
              instance.setContent(html);
            } 
          }).finally(() => {
            instance.enable();
            instance.show();
          });
        }
      } else {
        // See if we can fetch a full url (with no hash to target)
        // This is a special case and we should probably do some content thinning / targeting
        fetch(url)
        .then(res => res.text())
        .then(html => {
          const parser = new DOMParser();
          const htmlDoc = parser.parseFromString(html, "text/html");
          const note = htmlDoc.querySelector('main.content');
          if (note !== null) {
            // This should only happen for chapter cross references
            // (since there is no id in the URL)
            // remove the first header
            if (note.children.length > 0 && note.children[0].tagName === "HEADER") {
              note.children[0].remove();
            }
            const html = processXRef(null, note);
            instance.setContent(html);
          } 
        }).finally(() => {
          instance.enable();
          instance.show();
        });
      }
    }, function(instance) {
    });
  }
      let selectedAnnoteEl;
      const selectorForAnnotation = ( cell, annotation) => {
        let cellAttr = 'data-code-cell="' + cell + '"';
        let lineAttr = 'data-code-annotation="' +  annotation + '"';
        const selector = 'span[' + cellAttr + '][' + lineAttr + ']';
        return selector;
      }
      const selectCodeLines = (annoteEl) => {
        const doc = window.document;
        const targetCell = annoteEl.getAttribute("data-target-cell");
        const targetAnnotation = annoteEl.getAttribute("data-target-annotation");
        const annoteSpan = window.document.querySelector(selectorForAnnotation(targetCell, targetAnnotation));
        const lines = annoteSpan.getAttribute("data-code-lines").split(",");
        const lineIds = lines.map((line) => {
          return targetCell + "-" + line;
        })
        let top = null;
        let height = null;
        let parent = null;
        if (lineIds.length > 0) {
            //compute the position of the single el (top and bottom and make a div)
            const el = window.document.getElementById(lineIds[0]);
            top = el.offsetTop;
            height = el.offsetHeight;
            parent = el.parentElement.parentElement;
          if (lineIds.length > 1) {
            const lastEl = window.document.getElementById(lineIds[lineIds.length - 1]);
            const bottom = lastEl.offsetTop + lastEl.offsetHeight;
            height = bottom - top;
          }
          if (top !== null && height !== null && parent !== null) {
            // cook up a div (if necessary) and position it 
            let div = window.document.getElementById("code-annotation-line-highlight");
            if (div === null) {
              div = window.document.createElement("div");
              div.setAttribute("id", "code-annotation-line-highlight");
              div.style.position = 'absolute';
              parent.appendChild(div);
            }
            div.style.top = top - 2 + "px";
            div.style.height = height + 4 + "px";
            div.style.left = 0;
            let gutterDiv = window.document.getElementById("code-annotation-line-highlight-gutter");
            if (gutterDiv === null) {
              gutterDiv = window.document.createElement("div");
              gutterDiv.setAttribute("id", "code-annotation-line-highlight-gutter");
              gutterDiv.style.position = 'absolute';
              const codeCell = window.document.getElementById(targetCell);
              const gutter = codeCell.querySelector('.code-annotation-gutter');
              gutter.appendChild(gutterDiv);
            }
            gutterDiv.style.top = top - 2 + "px";
            gutterDiv.style.height = height + 4 + "px";
          }
          selectedAnnoteEl = annoteEl;
        }
      };
      const unselectCodeLines = () => {
        const elementsIds = ["code-annotation-line-highlight", "code-annotation-line-highlight-gutter"];
        elementsIds.forEach((elId) => {
          const div = window.document.getElementById(elId);
          if (div) {
            div.remove();
          }
        });
        selectedAnnoteEl = undefined;
      };
        // Handle positioning of the toggle
    window.addEventListener(
      "resize",
      throttle(() => {
        elRect = undefined;
        if (selectedAnnoteEl) {
          selectCodeLines(selectedAnnoteEl);
        }
      }, 10)
    );
    function throttle(fn, ms) {
    let throttle = false;
    let timer;
      return (...args) => {
        if(!throttle) { // first call gets through
            fn.apply(this, args);
            throttle = true;
        } else { // all the others get throttled
            if(timer) clearTimeout(timer); // cancel #2
            timer = setTimeout(() => {
              fn.apply(this, args);
              timer = throttle = false;
            }, ms);
        }
      };
    }
      // Attach click handler to the DT
      const annoteDls = window.document.querySelectorAll('dt[data-target-cell]');
      for (const annoteDlNode of annoteDls) {
        annoteDlNode.addEventListener('click', (event) => {
          const clickedEl = event.target;
          if (clickedEl !== selectedAnnoteEl) {
            unselectCodeLines();
            const activeEl = window.document.querySelector('dt[data-target-cell].code-annotation-active');
            if (activeEl) {
              activeEl.classList.remove('code-annotation-active');
            }
            selectCodeLines(clickedEl);
            clickedEl.classList.add('code-annotation-active');
          } else {
            // Unselect the line
            unselectCodeLines();
            clickedEl.classList.remove('code-annotation-active');
          }
        });
      }
  const findCites = (el) => {
    const parentEl = el.parentElement;
    if (parentEl) {
      const cites = parentEl.dataset.cites;
      if (cites) {
        return {
          el,
          cites: cites.split(' ')
        };
      } else {
        return findCites(el.parentElement)
      }
    } else {
      return undefined;
    }
  };
  var bibliorefs = window.document.querySelectorAll('a[role="doc-biblioref"]');
  for (var i=0; i<bibliorefs.length; i++) {
    const ref = bibliorefs[i];
    const citeInfo = findCites(ref);
    if (citeInfo) {
      tippyHover(citeInfo.el, function() {
        var popup = window.document.createElement('div');
        citeInfo.cites.forEach(function(cite) {
          var citeDiv = window.document.createElement('div');
          citeDiv.classList.add('hanging-indent');
          citeDiv.classList.add('csl-entry');
          var biblioDiv = window.document.getElementById('ref-' + cite);
          if (biblioDiv) {
            citeDiv.innerHTML = biblioDiv.innerHTML;
          }
          popup.appendChild(citeDiv);
        });
        return popup.innerHTML;
      });
    }
  }
});
</script>
</div> <!-- /content -->




<footer class="footer"><div class="nav-footer"><div class="nav-footer-center"><div class="toc-actions d-sm-block d-md-none"><ul><li><a href="https://github.com/AnswerDotAI/fasthtml/issues/new" class="toc-action"><i class="bi bi-github"></i>Report an issue</a></li></ul></div></div></div></footer></body></html>
'''

functions = scrape_fasthtml_api(html_content)

# Print the extracted information
for function in functions:
    print(f"Function: {function.name}")
    print(f"Signature: {function.signature}")
    print(f"Description: {function.description}")
    print("Parameters:")
    for param in function.parameters:
        print(f"  - {param.name}: {param.type} (default: {param.default})")
        print(f"    {param.details}")
    if function.returns:
        print(f"Returns: {function.returns.type}")
        print(f"  {function.returns.details}")
    print("\n" + "="*50 + "\n")
