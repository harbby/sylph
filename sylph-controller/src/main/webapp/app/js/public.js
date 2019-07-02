/**
 * Created by Polar on 2018/1/8.
 */
var publicData = {
    getBrowser: function () {
        var bro = '';
        var nav = navigator.userAgent.toLowerCase();
        if (!!window.ActiveXObject || "ActiveXObject" in window) {
            return "IE";
        }
        if (isFirefox = nav.indexOf("firefox") > 0) {
            return "Firefox";
        }
        if (isChrome = nav.indexOf("chrome") > 0 && window.chrome) {
            return "Chrome";
        }
        if (isSafari = nav.indexOf("safari") > 0 && nav.indexOf("version") > 0) {
            return "Safari";
        }
        if (isCamino = nav.indexOf("camino") > 0) {
            return "Camino";
        }
        if (window.opr != undefined) {
            return "Opera";
        }
        return bro;
    }
};
