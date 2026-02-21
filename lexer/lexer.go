package lexer

import (
	"github.com/walf443/oresql/token"
)

type Lexer struct {
	input   string
	pos     int  // current position in input (points to current char)
	readPos int  // current reading position (after current char)
	ch      byte // current char under examination
}

func New(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) peekChar() byte {
	if l.readPos >= len(l.input) {
		return 0
	}
	return l.input[l.readPos]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	start := l.pos
	for isLetter(l.ch) || isDigit(l.ch) {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readNumber() string {
	start := l.pos
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readString() string {
	l.readChar() // skip opening quote
	var result []byte
	for {
		if l.ch == 0 {
			break
		}
		if l.ch == '\'' {
			if l.peekChar() == '\'' {
				// escaped single quote
				result = append(result, '\'')
				l.readChar() // skip first quote
				l.readChar() // skip second quote
				continue
			}
			l.readChar() // skip closing quote
			break
		}
		result = append(result, l.ch)
		l.readChar()
	}
	return string(result)
}

func (l *Lexer) NextToken() token.Token {
	l.skipWhitespace()

	var tok token.Token

	switch l.ch {
	case 0:
		tok = token.Token{Type: token.EOF, Literal: ""}
	case '*':
		tok = token.Token{Type: token.ASTERISK, Literal: "*"}
		l.readChar()
	case '+':
		tok = token.Token{Type: token.PLUS, Literal: "+"}
		l.readChar()
	case '-':
		tok = token.Token{Type: token.MINUS, Literal: "-"}
		l.readChar()
	case '/':
		tok = token.Token{Type: token.SLASH, Literal: "/"}
		l.readChar()
	case ',':
		tok = token.Token{Type: token.COMMA, Literal: ","}
		l.readChar()
	case '(':
		tok = token.Token{Type: token.LPAREN, Literal: "("}
		l.readChar()
	case ')':
		tok = token.Token{Type: token.RPAREN, Literal: ")"}
		l.readChar()
	case ';':
		tok = token.Token{Type: token.SEMICOLON, Literal: ";"}
		l.readChar()
	case '.':
		tok = token.Token{Type: token.DOT, Literal: "."}
		l.readChar()
	case '=':
		tok = token.Token{Type: token.EQ, Literal: "="}
		l.readChar()
	case '<':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			tok = token.Token{Type: token.LT_EQ, Literal: "<="}
		} else if l.peekChar() == '>' {
			l.readChar()
			l.readChar()
			tok = token.Token{Type: token.NEQ, Literal: "<>"}
		} else {
			tok = token.Token{Type: token.LT, Literal: "<"}
			l.readChar()
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			tok = token.Token{Type: token.GT_EQ, Literal: ">="}
		} else {
			tok = token.Token{Type: token.GT, Literal: ">"}
			l.readChar()
		}
	case '!':
		if l.peekChar() == '=' {
			l.readChar()
			l.readChar()
			tok = token.Token{Type: token.NEQ, Literal: "!="}
		} else {
			tok = token.Token{Type: token.ILLEGAL, Literal: string(l.ch)}
			l.readChar()
		}
	case '\'':
		str := l.readString()
		tok = token.Token{Type: token.STRING_LIT, Literal: str}
	case '`':
		ident := l.readQuotedIdent()
		tok = token.Token{Type: token.QUOTED_IDENT, Literal: ident}
	default:
		if isLetter(l.ch) {
			literal := l.readIdentifier()
			tokType := token.LookupIdent(literal)
			tok = token.Token{Type: tokType, Literal: literal}
		} else if isDigit(l.ch) {
			num := l.readNumber()
			tok = token.Token{Type: token.INT_LIT, Literal: num}
		} else {
			tok = token.Token{Type: token.ILLEGAL, Literal: string(l.ch)}
			l.readChar()
		}
	}

	return tok
}

func (l *Lexer) readQuotedIdent() string {
	l.readChar() // skip opening backtick
	var result []byte
	for {
		if l.ch == 0 {
			break
		}
		if l.ch == '`' {
			if l.peekChar() == '`' {
				// escaped backtick
				result = append(result, '`')
				l.readChar() // skip first backtick
				l.readChar() // skip second backtick
				continue
			}
			l.readChar() // skip closing backtick
			break
		}
		result = append(result, l.ch)
		l.readChar()
	}
	return string(result)
}

func isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}
