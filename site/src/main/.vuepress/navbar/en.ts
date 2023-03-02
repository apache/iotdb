import { navbar } from 'vuepress-theme-hope';

export const enNavbar = navbar([
  {
    text: 'Documentation',
    children: [
      { text: 'latest', link: '/UserGuide/Master/QuickStart/QuickStart' },
      { text: 'v1.1.x', link: '/UserGuide/V1.1.x/QuickStart/QuickStart' },
      { text: 'v1.0.x', link: '/UserGuide/V1.0.x/QuickStart/QuickStart' },
      { text: 'v0.13.x', link: '/UserGuide/V0.13.x/QuickStart/QuickStart' },
    ],
  },
  {
    text: 'Design',
    link: 'https://cwiki.apache.org/confluence/display/IOTDB/System+Design',
  },
  {
    text: 'Download',
    link: '/Download/',
  },
  {
    text: 'Community',
    children: [
      { text: 'About', link: '/Community/About' },
      { text: 'Wiki', link: 'https://cwiki.apache.org/confluence/display/iotdb' },
      { text: 'People', link: '/Community/Community-Project  Committers' },
      { text: 'Powered  By', link: '/Community/Community-Powered  By' },
      { text: 'Resources', link: '/Community/Materials' },
      { text: 'Feedback', link: '/Community/Feedback' },
    ],
  },
  {
    text: 'Development',
    children: [
      { text: 'How  to  vote', link: '/Development/VoteRelease' },
      { text: 'How  to  Commit', link: '/Development/HowToCommit' },
      { text: 'Become  a  Contributor', link: '/Development/HowToJoin' },
      { text: 'Become  a  Committer', link: '/Development/Committer' },
      { text: 'ContributeGuide', link: '/Development/ContributeGuide' },
      { text: 'How  to  Contribute  Code', link: '/Development/HowtoContributeCode' },
      { text: 'Changelist  of  TsFile', link: '/Development/format-changelist' },
      { text: 'Changelist  of  RPC', link: '/Development/rpc-changelist' },
    ],
  },
  {
    text: 'ASF',
    children: [
      { text: 'Foundation', link: 'https://www.apache.org/' },
      { text: 'License', link: 'https://www.apache.org/licenses/' },
      { text: 'Security', link: 'https://www.apache.org/security/' },
      { text: 'Sponsorship', link: 'https://www.apache.org/foundation/sponsorship.html' },
      { text: 'Thanks', link: 'https://www.apache.org/foundation/thanks.html' },
      { text: 'Current  Events', link: 'https://www.apache.org/events/current-event' },
    ],
  },
]);
