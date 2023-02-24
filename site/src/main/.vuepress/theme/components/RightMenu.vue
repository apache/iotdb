<template>
  <div class="right-menu-wrapper">
    <div class="right-menu-margin">
      <div class="right-menu-content">
        <div
          :class="[
            'right-menu-item',
            'level' + item.level,
            { active: item.slug === hashText }
          ]"
          v-for="(item, i) in headers"
          :key="i"
        >
          <a :href="'#' + item.slug">{{ item.title }}</a>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  data() {
    return {
      headers: [],
      hashText: ''
    }
  },
  mounted() {
    this.getHeadersData()
    this.getHashText()
  },
  watch: {
    $route() {
      this.headers = this.$page.headers
      this.getHashText()
    }
  },
  methods: {
    getHeadersData() {
      this.headers = this.$page.headers
    },
    getHashText() {
      this.hashText = decodeURIComponent(window.location.hash.slice(1))
    }
  }
}
</script>

<style lang='stylus'>
.theme-style-line
  .right-menu-wrapper
    .right-menu-margin
      border-left 1px solid var(--borderColor)
.right-menu-wrapper
  width $rightMenuWidth
  float right
  // margin-right -($rightMenuWidth + 55px)
  // margin-top -($navbarHeight *2 + 1.5rem)
  position sticky
  top 0
  font-size 0.8rem
  .right-menu-margin
    margin-top: ($navbarHeight + 1rem)
    border-radius 3px
    overflow hidden
  .right-menu-title
    padding 10px 15px 0 15px
    background var(--mainBg)
    font-size 1rem
    &:after
      content ''
      display block
      width 100%
      height 1px
      background var(--borderColor)
      margin-top 10px
  .right-menu-content
    max-height 80vh
    position relative
    overflow hidden
    background var(--mainBg)
    padding 4px 3px 4px 0
    &::-webkit-scrollbar
      width 3px
      height 3px
    &::-webkit-scrollbar-track-piece
      background none
    &::-webkit-scrollbar-thumb:vertical
      background-color hsla(0, 0%, 49%, 0.3)
    &:hover
      overflow-y auto
      padding-right 0
    .right-menu-item
      padding 4px 15px
      // border-left 1px solid var(--borderColor)
      overflow hidden
      white-space nowrap
      text-overflow ellipsis
      position relative
      &.level2
        font-size 0.8rem
      &.level3
        padding-left 27px
      &.level4
        padding-left 37px
      &.level5
        padding-left 47px
      &.level6
        padding-left 57px
      &.active
        &:before
          content ''
          position absolute
          top 5px
          left 0
          width 3px
          height 14px
          background $accentColor
          border-radius 0 4px 4px 0
        a
          color $accentColor
          opacity 1
      a
        color var(--textColor)
        opacity 0.75
        display inline-block
        width 100%
        overflow hidden
        white-space nowrap
        text-overflow ellipsis
        &:hover
          opacity 1
      &:hover
        color $accentColor
</style>