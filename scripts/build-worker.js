/**
 * Post-build script: compile embedding.worker.ts separately
 * because webpack bundles everything into a single main.js
 * and doesn't include worker_threads files.
 */
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const workerSrc = path.join('apps', 'valuation', 'src', 'modules', 'export', 'services', 'embedding.worker.ts');
const outDir = path.join('dist', 'apps', 'valuation');

if (!fs.existsSync(workerSrc)) {
  console.log('embedding.worker.ts not found, skipping');
  process.exit(0);
}

// Compile with tsc to a temp dir, then copy the .js file
const tmpDir = path.join('dist', '_worker_tmp');
try {
  execSync(
    `npx tsc "${workerSrc}" --outDir "${tmpDir}" --module commonjs --target es2020 --esModuleInterop --skipLibCheck --resolveJsonModule`,
    { stdio: 'pipe' },
  );

  // Find the compiled file (tsc may or may not preserve directory structure)
  const compiled = path.join(tmpDir, 'embedding.worker.js');
  const compiledNested = path.join(tmpDir, 'apps', 'valuation', 'src', 'modules', 'export', 'services', 'embedding.worker.js');
  const found = [compiled, compiledNested].find(p => fs.existsSync(p));
  if (found) {
    fs.copyFileSync(found, path.join(outDir, 'embedding.worker.js'));
    console.log('✓ embedding.worker.js compiled and copied to dist');
  } else {
    console.warn('Warning: compiled worker not found at expected path');
  }
} catch (err) {
  console.warn('Warning: failed to compile embedding.worker.ts:', err.message);
} finally {
  // Cleanup temp dir
  fs.rmSync(tmpDir, { recursive: true, force: true });
}
