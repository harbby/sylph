// 移动端导航处理
(function(){
  'use strict';
  var mobileTrigger = document.getElementById('mobileTrigger');
  var mobileAside = document.getElementById('mobileAside');
  mobileTrigger.onclick = function(e) {
    if (mobileAside.className.indexOf('mobile-show') === -1) {
      mobileAside.className += ' mobile-show';
    } else {
      mobileAside.className = 'toc';
    }
  };
})();
