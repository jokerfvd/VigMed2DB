# encoding : utf-8 
#bibliotecas utilizadas
require 'sequel'
require 'pg'

puts "Vou conectar no banco"
#DB connection string!!!!!!!!!!!!!!!!!
DB = Sequel.connect 'postgres://{DBUSER}:{DBPASS}@{DBHOST}/{DATABASE}' 
puts "Conectado"

def fazInserts(inserts)
	if (inserts != nil)
		begin
			DB[$tabela].multi_insert(inserts)
		rescue => e 
			#$file.puts inserts
			#$file.puts e.message
			#$file.puts e.backtrace.inspect
			#$file.flush
			#$file.puts "Irei destrinchar os inserts um por um"
			inserts.each do |i|
				begin
					DB[$tabela].insert(i)
				rescue => e2
					#puts "Deu erro no insert: #{i}"
					$contaErro = $contaErro + 1
					$file.puts "#{e2.message.strip.delete("\n")}: #{i}"
					$file.flush
					next
				end
			end
		end
	end
end

def insere(arquivo) 
	nome = arquivo.split("_")[1].split(".")[0].strip.upcase  #pegando os 4 primeiro chars do nome do arquivo
	$file = File.open(__dir__+File::SEPARATOR+"saida-#{nome}.txt", 'w')
	puts "Nome da tabela #{nome}"
	first = true #pra saber quando for a primeira linha com o cabecalho
	headers = nil
	puts "Vou comecar a ler o arquivo #{arquivo}"
	tam = File.readlines(arquivo).size
	puts "O arquivo #{arquivo} tem #{tam} linhas"
	porcentInc = tam/100
	porcentI = 0
	i = 0
	inserts = nil
	multTam = 1000 #numero de tuplas por insert
	$tabela = nil
	$contaErro = 0
	File.readlines(arquivo, encoding: 'Windows-1252').each do |line|
		if ((i % multTam) == 0)
			fazInserts(inserts)
			inserts = Array.new
		end
		if ((i % porcentInc) == 0)
			puts "#{DateTime.now.strftime('%H:%M:%S')} #{porcentI}%"
			porcentI = porcentI + 1
		end
		if (first)
			headers = line.strip.split(";")
			first = false
		else
			#removendo # que tem no #UF
			aux = line.gsub(/\000/,'').strip.sub('#','').split(";")
			auxHash = Hash.new
			for auxi in 0..aux.length-1
				if (aux[auxi] == "")
					aux[auxi] = nil
				end
				if (headers[auxi] == "PESO_KG" && !aux[auxi].nil?)
					auxHash[headers[auxi].downcase] = aux[auxi].sub(",",".")
				else	
					auxHash[headers[auxi].downcase] = aux[auxi]
				end	
			end
			
			if (nome == "NOTIFICACOES")
				$tabela = :notificacoes
			elsif (nome == "MEDICAMENTOS")
				$tabela = :medicamentos
			elsif (nome == "REACOES")
				$tabela = :reacoes
			end
			inserts.push(auxHash)	
		end
		i = i + 1
	end
	if ( (tam % multTam) != 0 )#vai faltar o ultimo insert
		fazInserts(inserts)
	end
	$file.puts "Teve um total de #{$contaErro} erros em #{arquivo}"
	$file.close
end

=begin
Dir.glob("*.csv").each do |arq|
	insere(arq.strip)
end
=end
insere("VigiMed_Notificacoes.csv")
insere("VigiMed_Medicamentos.csv")
insere("VigiMed_Reacoes.csv")

puts "Fim"
