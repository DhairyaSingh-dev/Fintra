/** @type {import('jest').Config} */
export default {
  testEnvironment: 'jsdom',
  moduleFileExtensions: ['js'],
  testMatch: ['**/__tests__/**/*.test.js'],
  transform: {},
  collectCoverageFrom: [
    'static/**/*.js',
    '!static/__tests__/**'
  ],
  transformIgnorePatterns: [
    '/node_modules/'
  ],
  setupFilesAfterEnv: [],
  verbose: true
};
