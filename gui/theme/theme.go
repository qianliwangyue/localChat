package theme

import (
	_ "embed"
	"image/color"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/theme"
)

var (
	//go:embed fonts/HYQingZhouXingJ-2.ttf
	NotoSansSC []byte
)

// 现代化配色：靛蓝品牌色 + 柔和的中性灰背景，同时适配浅色/深色两种模式。
var (
	brandPrimary   = color.NRGBA{R: 0x4C, G: 0x6E, B: 0xF5, A: 0xFF} // 靛蓝 #4C6EF5
	brandPrimaryDk = color.NRGBA{R: 0x6E, G: 0x87, B: 0xFF, A: 0xFF} // 深色模式下更亮的靛蓝
	onPrimary      = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}
	onPrimaryDark  = color.NRGBA{R: 0x0F, G: 0x13, B: 0x20, A: 0xFF}

	// 浅色模式
	bgLight       = color.NRGBA{R: 0xF1, G: 0xF2, B: 0xF6, A: 0xFF} // 页面背景 #F1F2F6
	surfaceLight  = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF} // 卡片/输入框 #FFFFFF
	fgLight       = color.NRGBA{R: 0x1A, G: 0x1D, B: 0x26, A: 0xFF} // 主文字
	phLight       = color.NRGBA{R: 0x98, G: 0xA0, B: 0xB3, A: 0xFF} // 占位符/次要文字
	sepLight      = color.NRGBA{R: 0xE4, G: 0xE6, B: 0xEE, A: 0xFF} // 分隔线
	hoverLight    = color.NRGBA{R: 0xE9, G: 0xEB, B: 0xF3, A: 0xFF}
	selLight      = color.NRGBA{R: 0xDE, G: 0xE6, B: 0xFF, A: 0xFF}
	disBtnLight   = color.NRGBA{R: 0xE9, G: 0xEB, B: 0xF1, A: 0xFF}
	focusLight    = color.NRGBA{R: 0x4C, G: 0x6E, B: 0xF5, A: 0x28}
	pressedLight  = color.NRGBA{R: 0x4C, G: 0x6E, B: 0xF5, A: 0x30}
	scrollLight   = color.NRGBA{R: 0x1A, G: 0x1D, B: 0x26, A: 0x30}
	shadowLight   = color.NRGBA{R: 0x0F, G: 0x14, B: 0x28, A: 0x14}
	inputBdLight  = color.NRGBA{R: 0xDD, G: 0xE0, B: 0xEA, A: 0xFF}
	successLight  = color.NRGBA{R: 0x10, G: 0xB9, B: 0x81, A: 0xFF} // 在线状态点 #10B981
	warningLight  = color.NRGBA{R: 0xF5, G: 0x9E, B: 0x0B, A: 0xFF}
	errorLight    = color.NRGBA{R: 0xEF, G: 0x44, B: 0x44, A: 0xFF}
	linkLight     = color.NRGBA{R: 0x3B, G: 0x5B, B: 0xDB, A: 0xFF}
	overlayLight  = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}
	menuLight     = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}
	scrollBgLight = color.NRGBA{R: 0x00, G: 0x00, B: 0x00, A: 0x04}

	// 深色模式
	bgDark       = color.NRGBA{R: 0x15, G: 0x17, B: 0x1D, A: 0xFF}
	surfaceDark  = color.NRGBA{R: 0x1D, G: 0x20, B: 0x27, A: 0xFF}
	fgDark       = color.NRGBA{R: 0xE7, G: 0xEA, B: 0xF2, A: 0xFF}
	phDark       = color.NRGBA{R: 0x6D, G: 0x74, B: 0x84, A: 0xFF}
	sepDark      = color.NRGBA{R: 0x2A, G: 0x2E, B: 0x3A, A: 0xFF}
	hoverDark    = color.NRGBA{R: 0x26, G: 0x2B, B: 0x36, A: 0xFF}
	selDark      = color.NRGBA{R: 0x2F, G: 0x3A, B: 0x5E, A: 0xFF}
	disBtnDark   = color.NRGBA{R: 0x20, G: 0x24, B: 0x2E, A: 0xFF}
	focusDark    = color.NRGBA{R: 0x6E, G: 0x87, B: 0xFF, A: 0x32}
	pressedDark  = color.NRGBA{R: 0x6E, G: 0x87, B: 0xFF, A: 0x38}
	scrollDark   = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0x1E}
	shadowDark   = color.NRGBA{R: 0x00, G: 0x00, B: 0x00, A: 0x40}
	inputBdDark  = color.NRGBA{R: 0x2A, G: 0x2E, B: 0x3A, A: 0xFF}
	successDark  = color.NRGBA{R: 0x2E, G: 0xC9, B: 0x93, A: 0xFF}
	warningDark  = color.NRGBA{R: 0xF7, G: 0xB7, B: 0x33, A: 0xFF}
	errorDark    = color.NRGBA{R: 0xF8, G: 0x71, B: 0x71, A: 0xFF}
	linkDark     = color.NRGBA{R: 0x8F, G: 0xA8, B: 0xFF, A: 0xFF}
	overlayDark  = color.NRGBA{R: 0x22, G: 0x26, B: 0x30, A: 0xFF}
	menuDark     = color.NRGBA{R: 0x1D, G: 0x20, B: 0x27, A: 0xFF}
	scrollBgDark = color.NRGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0x05}
)

type MyTheme struct{}

var _ fyne.Theme = (*MyTheme)(nil)

// Font StaticName 为 fonts 目录下的 ttf 类型的字体文件名
func (m MyTheme) Font(fyne.TextStyle) fyne.Resource {
	return &fyne.StaticResource{
		StaticName:    "HYQingZhouXingJ-2.ttf",
		StaticContent: NotoSansSC,
	}
}

func (*MyTheme) Color(n fyne.ThemeColorName, v fyne.ThemeVariant) color.Color {
	light := v == theme.VariantLight
	switch n {
	case theme.ColorNamePrimary:
		if light {
			return brandPrimary
		}
		return brandPrimaryDk
	case theme.ColorNameForegroundOnPrimary:
		if light {
			return onPrimary
		}
		return onPrimaryDark
	case theme.ColorNameBackground:
		if light {
			return bgLight
		}
		return bgDark
	case theme.ColorNameHeaderBackground, theme.ColorNameMenuBackground:
		if light {
			return menuLight
		}
		return menuDark
	case theme.ColorNameOverlayBackground:
		if light {
			return overlayLight
		}
		return overlayDark
	case theme.ColorNameForeground:
		if light {
			return fgLight
		}
		return fgDark
	case theme.ColorNamePlaceHolder:
		if light {
			return phLight
		}
		return phDark
	case theme.ColorNameInputBackground:
		if light {
			return surfaceLight
		}
		return surfaceDark
	case theme.ColorNameInputBorder:
		if light {
			return inputBdLight
		}
		return inputBdDark
	case theme.ColorNameButton:
		if light {
			return surfaceLight
		}
		return surfaceDark
	case theme.ColorNameDisabledButton:
		if light {
			return disBtnLight
		}
		return disBtnDark
	case theme.ColorNameHover:
		if light {
			return hoverLight
		}
		return hoverDark
	case theme.ColorNameSelection:
		if light {
			return selLight
		}
		return selDark
	case theme.ColorNameFocus:
		if light {
			return focusLight
		}
		return focusDark
	case theme.ColorNamePressed:
		if light {
			return pressedLight
		}
		return pressedDark
	case theme.ColorNameSeparator:
		if light {
			return sepLight
		}
		return sepDark
	case theme.ColorNameScrollBar:
		if light {
			return scrollLight
		}
		return scrollDark
	case theme.ColorNameScrollBarBackground:
		if light {
			return scrollBgLight
		}
		return scrollBgDark
	case theme.ColorNameShadow:
		if light {
			return shadowLight
		}
		return shadowDark
	case theme.ColorNameSuccess:
		if light {
			return successLight
		}
		return successDark
	case theme.ColorNameWarning:
		if light {
			return warningLight
		}
		return warningDark
	case theme.ColorNameError:
		if light {
			return errorLight
		}
		return errorDark
	case theme.ColorNameHyperlink:
		if light {
			return linkLight
		}
		return linkDark
	case theme.ColorNameDisabled:
		if light {
			return color.NRGBA{R: 0xB0, G: 0xB6, B: 0xC4}
		}
		return color.NRGBA{R: 0x54, G: 0x5A, B: 0x68}
	default:
		return theme.DefaultTheme().Color(n, v)
	}
}

func (*MyTheme) Icon(n fyne.ThemeIconName) fyne.Resource {
	return theme.DefaultTheme().Icon(n)
}

func (*MyTheme) Size(n fyne.ThemeSizeName) float32 {
	switch n {
	// 统一的大圆角：输入框、列表选中项、按钮等呈现现代 App 的柔和观感
	case theme.SizeNameInputRadius, theme.SizeNameSelectionRadius:
		return 10
	case theme.SizeNamePadding:
		return 5
	default:
		return theme.DefaultTheme().Size(n)
	}
}
