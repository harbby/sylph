'use strict';

const path = require('path');
const renderer = require('../lib/renderer');
const root = "/project/sylph/"

/* global hexo */

hexo.extend.renderer.register('md', 'html', renderer, true);

hexo.extend.helper.register('root_link', function() {
    return root
});

// 左侧导航
hexo.extend.helper.register('guide_toc', function() {
  const type = this.page.canonical_path.split('/')[0];
  const sidebar = this.site.data.sidebar[type];
  var prefix = 'sidebar.' + type + '.';
  // const toc = this.site.data.guide_toc;
  const toc = sidebar;
  let menu = '<dl>';
  for (let title in toc) {
    const subMenu = toc[title];
    title = getI18nText(this.__, title, prefix);
    menu += `<dt>${title}</dt><dd><ul>`;
    for (let subTitle in subMenu) {
      const curPath = this.page.lang + subMenu[subTitle];
      const url = root + curPath;
      const itemClass = curPath === this.path ? 'cur' : '';
      subTitle = getI18nText(this.__, subTitle, prefix);
      // add current class 注意 basename是可能重复的
      menu += `<li><a href="${url}" class="${itemClass}">${subTitle}</a></li>`;
    }
    menu += '</ul></dd>';
  }

  menu += '</dl>';
  return menu;
});

hexo.extend.helper.register('menu_link', function() {
  const menus = this.site.data.menu;
  let links = '';
  for (const menu in menus) {
    let link = menus[menu];
    const content = getI18nText(this.__, menu, 'menu.');
    let curPath = this.page.lang + link;
    // 兼容没有 index.html 的情况
    if(!~curPath.indexOf('.html')) {
      curPath += 'index.html'
    }
    const itemClass = curPath === this.path ? 'cur' : '';
    if (this.page.lang !== 'en' && !/^http/.test(link)) {
      link = root + this.page.lang + link;
    }
    if (this.page.lang == 'en' && !/^http/.test(link)) {  //兼容en
      link = root + this.page.lang + link;
    }
    links += `<li><a href="${link}" alt="${content}" class="${itemClass}">${content}</a></li>`;
  }

  return links;
});

// 返回首页
hexo.extend.helper.register('index_link', function(url) {
  if (!url) {
    url = "/";
  }
  if (this.page.lang !== 'en') {
    return root + `${this.page.lang}${url}`;
  }
  if (this.page.lang == 'en') {  //兼容en
        return root + `${this.page.lang}${url}`;
  }
  return root + url;
});

function getI18nText(gettext, text, prefix) {
  const key = prefix + text;
  const tmp = gettext(key);
  return tmp === key ? text : tmp;
}
