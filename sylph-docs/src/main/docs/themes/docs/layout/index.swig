<div class="index">
  <div class="banner block index-bg-light">
    <div class="banner-logo">
      <!-- <img src="https://zos.alipayobjects.com/rmsportal/JFKAMfmPehWfhBPdCjrw.svg"/> -->
    </div>
    <div class="banner-info">
      <h1 class="fadeInUp animated"><p style="font-weight:bold;font-size:48px;">STREAM FrameWork</p>{{ page.doc }}</h1>
      <p style="font-size: 22px;" class="fadeInUp animated">{{ page.subtitle }}</p>
      <p class="banner-button fadeInUp animated">
        <a class="btn btn-primary" href="{{ index_link('/docs/intro/quickstart.html') }}">{{ __('index.getstart') }}</a>
        <a class="btn btn-secondary" href="https://github.com/harbby/sylph">code</a>
        <a class="btn btn-download" href="https://github.com/harbby/sylph/releases">download</a>
      </p>
    </div>
  </div>
</div>
{# 移动端导航需要 #}
<div class="index-aside">
{{ partial('partial/aside') }}
</div>
{{ partial('partial/footer') }}


