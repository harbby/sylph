<div class="nav" >
  <header>
    <a href="{{ index_link() }}" class="nav-logo" alt="nodeui">
    <img src="{{ root_link() }}images/logo.png" width="89"><span>SYLPH</span>
    </a>
    <ul class="nav-item">
      <li>
        <form id="search-form">
          <input type="text" id="search-query" class="search-query st-default-search-input">
        </form>
      </li>
      {{ menu_link() }}

      <li class="translations">
        <a class="nav-link">{{ __('header.translations') }}</a>
        <span class="arrow"></span>
        <ul class="dropdown-content">
          <li><a href="/project/sylph/zh-cn">中文</a></li>
          <li><a href="/project/sylph/en">English</a></li>
        </ul>
      </li>

      <!-- github star星数 -->
      <li>
          <iframe src="https://ghbtns.com/github-btn.html?user=harbby&repo=sylph&type=star&count=true" frameborder="0" scrolling="0" width="80px" height="20px"></iframe>
          <iframe src="https://ghbtns.com/github-btn.html?user=harbby&repo=sylph&type=fork&count=true" frameborder="0" scrolling="0" width="80px" height="20px"></iframe>
      </li>

      {#
        <!-- 注释 -->
      #}

    </ul>
    <a id="mobileTrigger" href="#" class="mobile-trigger">
      <ul>
        <li></li>
        <li></li>
        <li></li>
      </ul>
    </a>
  </header>
</div>
