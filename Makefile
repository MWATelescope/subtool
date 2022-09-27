all:
	npx tsc --target es2022 layout.ts util.ts repoint.ts
	mv util.js util.mjs
	mv layout.js layout.mjs
	mv repoint.js repoint.mjs

