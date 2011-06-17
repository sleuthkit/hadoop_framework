var knownBad = [
	{a: "E4D909C290D0FB1CA068FFADDF22C9D0", fId: "01AB93CF", fN: "malware.exe", fP: "vol1\\Windows\\system32\\malware.exe", h: "Malware", d: "some bad file", av: "infected/Password.Stealer", seen: [ {a: "AR-2010-01-B436128"}, {a: "AR-2010-02-C866229"} ] },
	{a: "A4D909C290D0FB1CA068FFADDF22C77D", fId: "02AB93CF", fN: "trojan.exe", fP: "vol1\\Windows\\system32\\trojan.exe", h: "Malware", d: "some bad file", av: "infected/Trojan.Malware", seen: [ {a: "AR-2010-01-B436128"}, {a: "AR-2010-05-C866229"} ] },
	{a: "44D909C290D0FB1CA068FFADDF22C256", fId: "03AB93CF", fN: "badguys.doc", fP: "vol1\\Documents and Settings\\john\\My Documents\\badguys.doc", h: "Terrorism", d: "some bad file", av: "clean", seen: [] },
	{a: "D4D909C290D0FB1CA068FFADDF22CB36", fId: "04AB93CF", fN: "targets.xlsx", fP: "vol1\\Documents and Settings\\john\\My Documents\\Targets\\targets.xlsx", h: "Terrorism", d: "some bad file", av: "clean", seen: [ {a: "AR-2010-02-B436128"}, {a: "AR-2010-09-C866229"} ] },
	{a: "C4D909C290D0FB1CA068FFADDF22CD85", fId: "05AB93CF", fN: "compound map.jpg", fP: "vol1\\Documents and Settings\\john\\My Documents\\Plans\\compound map.jpg", h: "Terrorism", d: "some bad file", av: "clean", seen: [ {a: "AR-2010-02-C866229"} ] },
	{a: "34D909C290D0FB1CA068FFADDF22CCA8", fId: "06AB93CF", fN: "grocery list4.txt", fP: "vol1\\Documents and Settings\\john\\My Documents\\grocery list4.txt", h: "Terrorism", d: "some bad file", av: "clean", seen: [ {a: "AR-2010-01-B436128"}, {a: "AR-2010-08-C866229"} ] },
	{a: "84D909C290D0FB1CA068FFADDF22CAC3", fId: "07AB93CF", fN: "dropper.exe", fP: "vol1\\Windows\\system32\\dropper.exe", h: "Malware", d: "some bad file", av: "infected/Dropper", seen: [ {a: "AR-2010-01-B436128"} ] },
	{a: "64D909C290D0FB1CA068FFADDF22C5BF", fId: "08AB93CF", fN: "compromise.dll", fP: "vol1\\Windows\\system32\\compromise.dll", h: "Malware", d: "some bad file", av: "infected/AdBot", seen: [ {a: "AR-2010-06-C866229"} ] },
	{a: "14D909C290D0FB1CA068FFADDF22C2BA", fId: "09AB93CF", fN: "kitty.jpg", fP: "vol1\\Documents and Settings\\john\\My Documents\\My Pictures\\kitty.jpg", h: "Pornography", d: "some bad file", av: "clean", seen: [ {a: "AR-2010-04-B436128"}, {a: "AR-2010-09-C866229"} ] }
];

var knownBadCounts = [
  {a: "Malware", n: 16},
  {a: "Terrorism", n: 13},
  {a: "Pornography", n: 4}
];