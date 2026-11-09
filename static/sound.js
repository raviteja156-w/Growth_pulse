/* GrowthPulse sound: tiny WebAudio effects + read-aloud. No audio files needed. */
(function(){
var KEY='gp_sound',ctx=null;
function on(){try{return localStorage.getItem(KEY)!=='off'}catch(e){return true}}
function setOn(v){try{localStorage.setItem(KEY,v?'on':'off')}catch(e){}paint();if(!v&&window.speechSynthesis)speechSynthesis.cancel()}
function ac(){if(!ctx){var C=window.AudioContext||window.webkitAudioContext;if(!C)return null;try{ctx=new C()}catch(e){return null}}if(ctx.state==='suspended')ctx.resume();return ctx}
function tone(f,t0,d,type,vol){var c=ac();if(!c)return;var o=c.createOscillator(),g=c.createGain();o.type=type||'sine';o.frequency.value=f;
var s=c.currentTime+t0;g.gain.setValueAtTime(0.0001,s);g.gain.exponentialRampToValueAtTime(vol||0.08,s+0.01);g.gain.exponentialRampToValueAtTime(0.0001,s+d);o.connect(g);g.connect(c.destination);o.start(s);o.stop(s+d+0.02)}
var FX={tap:function(){tone(660,0,.07,'sine',.05)},
ok:function(){tone(523,0,.12);tone(659,.11,.12);tone(784,.22,.2)},
err:function(){tone(220,0,.18,'square',.04);tone(165,.16,.25,'square',.04)},
send:function(){tone(500,0,.08);tone(750,.07,.1)},
recv:function(){tone(880,0,.1,'triangle');tone(1175,.09,.16,'triangle')},
done:function(){[523,659,784,1047].forEach(function(f,i){tone(f,i*.12,.2)})}};
function play(n){if(!on())return;try{(FX[n]||FX.tap)()}catch(e){}}
function speak(text,btn){if(!window.speechSynthesis){return}
if(speechSynthesis.speaking){speechSynthesis.cancel();if(btn&&btn.dataset.sp==='1'){btn.dataset.sp='0';btn.textContent='🔊 Listen';return}}
var u=new SpeechSynthesisUtterance(String(text).replace(/\s+/g,' ').slice(0,3000));u.lang='en-IN';u.rate=.95;
if(btn){document.querySelectorAll('.gp-listen').forEach(function(b){b.dataset.sp='0';b.textContent='🔊 Listen'});btn.dataset.sp='1';btn.textContent='⏹ Stop';u.onend=u.onerror=function(){btn.dataset.sp='0';btn.textContent='🔊 Listen'}}
speechSynthesis.speak(u)}
function paint(){var b=document.getElementById('gp-mute');if(b){b.textContent=on()?'🔊':'🔇';b.title=on()?'Sound on (tap to mute)':'Sound off (tap to unmute)'}}
function addListen(root){(root||document).querySelectorAll('.paper').forEach(function(p){
if(p.dataset.gl||p.closest('.nodl'))return;p.dataset.gl='1';
var b=document.createElement('button');b.type='button';b.className='gp-listen';b.textContent='🔊 Listen';b.dataset.sp='0';
b.style.cssText='display:block;margin:.2rem 0 .3rem auto;font:600 .75rem system-ui,sans-serif;padding:.25rem .6rem;border-radius:999px;border:1px solid #1d4ed8;background:#eff6ff;color:#1d4ed8;cursor:pointer';
b.onclick=function(e){e.stopPropagation();if(!on())setOn(true);speak(p.innerText.replace('🔊 Listen','').replace('⏹ Stop',''),b)};
p.insertBefore(b,p.firstChild)})}
document.addEventListener('click',function(e){var t=e.target.closest&&e.target.closest('.btn,button,summary,.navlinks a,.hv');if(t&&!t.classList.contains('gp-listen')&&t.id!=='gp-mute')play('tap')},true);
document.addEventListener('DOMContentLoaded',function(){
var m=document.getElementById('gp-mute');if(m)m.onclick=function(){var v=!on();setOn(v);if(v)play('ok')};paint();addListen();
document.querySelectorAll('.toast').forEach(function(t){setTimeout(function(){play(t.className.indexOf('emerald')>-1?'ok':'err')},250)})});
window.GPSound={play:play,speak:speak,on:on,setOn:setOn,addListen:addListen};
})();
