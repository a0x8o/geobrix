/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  // By default, Docusaurus generates a sidebar from the docs folder structure
  tutorialSidebar: [
    'intro',
    'installation',
    'quick-start',
    'databricks-spatial',
    'beta-release-notes',
    {
      type: 'category',
      label: 'Notebooks',
      collapsed: false,
      items: [
        'notebooks/eo-series',
        'notebooks/xview',
      ],
    },
    {
      type: 'category',
      label: 'Sample Data',
      collapsed: false,
      items: [
        'sample-data/overview',
        'sample-data/setup',
        'sample-data/vector-data',
        'sample-data/raster-data',
        'sample-data/additional',
      ],
    },
    {
      type: 'category',
      label: 'Readers & Writers',
      collapsed: false,
      items: [
        'readers/overview',
        'writers/overview',
        {
          type: 'category',
          label: 'Readers',
          collapsed: false,
          items: [
            { type: 'category', label: 'General', collapsed: false, items: ['readers/raster', 'readers/vector'] },
            { type: 'category', label: 'Named', collapsed: false, items: ['readers/geotiff', 'readers/shapefile', 'readers/geojson', 'readers/geopackage', 'readers/filegdb'] },
          ],
        },
        {
          type: 'category',
          label: 'Writers',
          collapsed: false,
          items: [
            { type: 'category', label: 'General', collapsed: false, items: ['writers/raster'] },
            { type: 'category', label: 'Named', collapsed: false, items: ['writers/geotiff', 'writers/pmtiles'] },
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Functions',
      collapsed: false,
      items: [
        'api/overview',
        'api/tile-structure',
        {
          type: 'category',
          label: 'Function Reference',
          collapsed: false,
          items: [
            'api/execution-tiers',
            'api/benchmarking',
            'api/raster-functions',
            'api/raster-functions-heavyweight',
            'api/gridx-functions',
            'api/vectorx-functions',
            'api/pmtiles-functions',
          ],
        },
        'api/scala',
        'api/python',
        'api/sql',
      ],
    },
    // Temporarily hidden until Examples section is ready to ship
    // {
    //   type: 'category',
    //   label: 'Examples',
    //   items: [
    //     'examples/overview',
    //   ],
    // },
    {
      type: 'category',
      label: 'Advanced Usage',
      collapsed: false,
      items: [
        'advanced/overview',
        'advanced/custom-udfs',
        'advanced/gdal-cli',
        'advanced/library-integration',
      ],
    },
    'developers',
    'security',
    'limitations',
    'support',
  ],
};

export default sidebars;

