<title>{{ page.title || 'SYLPH' }} - {{ __('index.slogan') }}</title>
<meta charset="utf-8">
<meta name="description" content="{{ __('') }}">
<meta http-equiv="X-UA-Compatible" content="IE=Edge,chrome=1">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<link rel="icon" href="{{ root_link() }}images/favicon.png" type="image/x-icon">
{# 加载带版本号的css
	<link href="{{ url_for(theme.css) }}/main.css?v={{ theme.version }}" rel="stylesheet" type="text/css" />
#}

{{ css('css/index') }}

{#
	<!-- <link rel="stylesheet" href="https://cdn.jsdelivr.net/docsearch.js/2/docsearch.min.css" /> -->
#}

