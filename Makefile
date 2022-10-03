all:
	npx tsc --target es2022 dt.ts dump.ts layout.ts repoint.ts subfile.ts util.ts
	mv dt.js dt.mjs
	mv dump.js dump.mjs
	mv util.js util.mjs
	mv layout.js layout.mjs
	mv repoint.js repoint.mjs
	mv types.js types.mjs
	mv subfile.js subfile.mjs


