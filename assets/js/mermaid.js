import{c as constant,p as path}from"./constant-b644328d.js";import{O as pi,P as cos,Q as sin,R as halfPi,T as epsilon,K as tau,V as sqrt,W as min,X as abs,Y as atan2,Z as asin,_ as acos,$ as max}from"./utils-1aebe9b6.js";function arcInnerRadius(n){return n.innerRadius}function arcOuterRadius(n){return n.outerRadius}function arcStartAngle(n){return n.startAngle}function arcEndAngle(n){return n.endAngle}function arcPadAngle(n){return n&&n.padAngle}function intersect(n,t,a,s,c,r,e,i){var a=a-n,s=s-t,e=e-c,i=i-r,o=i*a-e*s;if(!(o*o<epsilon))return[n+(o=(e*(t-r)-i*(n-c))/o)*a,t+o*s]}function cornerTangents(n,t,a,s,c,r,e){var i=n-a,o=t-s,e=(e?r:-r)/sqrt(i*i+o*o),o=e*o,e=-e*i,i=n+o,n=t+e,t=a+o,a=s+e,s=(i+t)/2,l=(n+a)/2,u=t-i,p=a-n,y=u*u+p*p,r=c-r,i=i*a-t*n,a=(p<0?-1:1)*sqrt(max(0,r*r*y-i*i)),t=(i*p-u*a)/y,n=(-i*u-p*a)/y,f=(i*p+u*a)/y,i=(-i*u+p*a)/y,u=t-s,p=n-l,a=f-s,y=i-l;return a*a+y*y<u*u+p*p&&(t=f,n=i),{cx:t,cy:n,x01:-o,y01:-e,x11:t*(c/r-1),y11:n*(c/r-1)}}function d3arc(){var E=arcInnerRadius,I=arcOuterRadius,S=constant(0),K=null,Q=arcStartAngle,V=arcEndAngle,W=arcPadAngle,X=null;function t(){var n,t,a,s,c,r,e,i,o,l,u,p,y,f,x,g,h,d,m,T,A,v,R=+E.apply(this,arguments),q=+I.apply(this,arguments),P=Q.apply(this,arguments)-halfPi,b=V.apply(this,arguments)-halfPi,O=abs(b-P),j=P<b;if(X=X||(n=path()),q<R&&(t=q,q=R,R=t),epsilon<q?tau-epsilon<O?(X.moveTo(q*cos(P),q*sin(P)),X.arc(0,0,q,P,b,!j),epsilon<R&&(X.moveTo(R*cos(b),R*sin(b)),X.arc(0,0,R,b,P,j))):(s=t=P,c=a=b,e=r=O,u=W.apply(this,arguments)/2,y=epsilon<u&&(K?+K.apply(this,arguments):sqrt(R*R+q*q)),v=A=i=min(abs(q-R)/2,+S.apply(this,arguments)),epsilon<y&&(p=asin(y/R*sin(u)),y=asin(y/q*sin(u)),(r-=2*p)>epsilon?(s+=p*=j?1:-1,c-=p):(r=0,s=c=(P+b)/2),(e-=2*y)>epsilon?(t+=y*=j?1:-1,a-=y):(e=0,t=a=(P+b)/2)),u=q*cos(t),p=q*sin(t),y=R*cos(c),P=R*sin(c),epsilon<i&&(f=q*cos(a),x=q*sin(a),g=R*cos(s),h=R*sin(s),O<pi&&(b=intersect(u,p,g,h,f,x,y,P))&&(O=u-b[0],T=p-b[1],d=f-b[0],m=x-b[1],O=1/sin(acos((O*d+T*m)/(sqrt(O*O+T*T)*sqrt(d*d+m*m)))/2),T=sqrt(b[0]*b[0]+b[1]*b[1]),A=min(i,(R-T)/(O-1)),v=min(i,(q-T)/(1+O)))),epsilon<e?epsilon<v?(o=cornerTangents(g,h,u,p,q,v,j),l=cornerTangents(f,x,y,P,q,v,j),X.moveTo(o.cx+o.x01,o.cy+o.y01),v<i?X.arc(o.cx,o.cy,v,atan2(o.y01,o.x01),atan2(l.y01,l.x01),!j):(X.arc(o.cx,o.cy,v,atan2(o.y01,o.x01),atan2(o.y11,o.x11),!j),X.arc(0,0,q,atan2(o.cy+o.y11,o.cx+o.x11),atan2(l.cy+l.y11,l.cx+l.x11),!j),X.arc(l.cx,l.cy,v,atan2(l.y11,l.x11),atan2(l.y01,l.x01),!j))):(X.moveTo(u,p),X.arc(0,0,q,t,a,!j)):X.moveTo(u,p),epsilon<R&&epsilon<r?epsilon<A?(o=cornerTangents(y,P,f,x,R,-A,j),l=cornerTangents(u,p,g,h,R,-A,j),X.lineTo(o.cx+o.x01,o.cy+o.y01),A<i?X.arc(o.cx,o.cy,A,atan2(o.y01,o.x01),atan2(l.y01,l.x01),!j):(X.arc(o.cx,o.cy,A,atan2(o.y01,o.x01),atan2(o.y11,o.x11),!j),X.arc(0,0,R,atan2(o.cy+o.y11,o.cx+o.x11),atan2(l.cy+l.y11,l.cx+l.x11),j),X.arc(l.cx,l.cy,A,atan2(l.y11,l.x11),atan2(l.y01,l.x01),!j))):X.arc(0,0,R,c,s,j):X.lineTo(y,P)):X.moveTo(0,0),X.closePath(),n)return X=null,n+""||null}return t.centroid=function(){var n=(+E.apply(this,arguments)+ +I.apply(this,arguments))/2,t=(+Q.apply(this,arguments)+ +V.apply(this,arguments))/2-pi/2;return[cos(t)*n,sin(t)*n]},t.innerRadius=function(n){return arguments.length?(E="function"==typeof n?n:constant(+n),t):E},t.outerRadius=function(n){return arguments.length?(I="function"==typeof n?n:constant(+n),t):I},t.cornerRadius=function(n){return arguments.length?(S="function"==typeof n?n:constant(+n),t):S},t.padRadius=function(n){return arguments.length?(K=null==n?null:"function"==typeof n?n:constant(+n),t):K},t.startAngle=function(n){return arguments.length?(Q="function"==typeof n?n:constant(+n),t):Q},t.endAngle=function(n){return arguments.length?(V="function"==typeof n?n:constant(+n),t):V},t.padAngle=function(n){return arguments.length?(W="function"==typeof n?n:constant(+n),t):W},t.context=function(n){return arguments.length?(X=null==n?null:n,t):X},t}export{d3arc as d};